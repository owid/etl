"""Export step that generates JSON files for the UN migration flows visualization.

This step reads the migration_stock_flows garden dataset and generates:
* A metadata JSON file with entity/gender dimensions and time range
* Individual data JSON files per country/entity

Each per-country file contains all pairwise immigrant and emigrant stocks for
that country, encoded as parallel arrays indexed by entity IDs from the metadata.

Outputs:
* Files are saved locally and (unless DRY_RUN) uploaded to S3 at:
  * https://owid-public.owid.io/data/migration/migration-stock-flows.metadata.json
  * https://owid-public.owid.io/data/migration/migration-stock-flows.<entityId>.json

Run with DRY_RUN=1 to skip S3 upload and only write local files:
  DRY_RUN=1 .venv/bin/etlr export://s3/un_migration/latest/migration_stock_flows_json
"""

import json
from pathlib import Path

import pandas as pd
from owid.catalog import Table, s3_utils
from structlog import get_logger
from tqdm.auto import tqdm

from etl.config import DRY_RUN
from etl.helpers import PathFinder
from etl.paths import EXPORT_DIR

log = get_logger()

S3_BUCKET_NAME = "owid-public"
S3_DATA_DIR = Path("data/migration/")

# load regions from stocks_regions.yml
REGIONS = [
    # UN statistical regions
    "Central Asia (UN)",
    "Central and Southern Asia (UN)",
    "Central America (UN)",
    "Eastern Africa (UN)",
    "Eastern Asia (UN)",
    "Eastern Europe (UN)",
    "Eastern and South-Eastern Asia (UN)",
    "Europe and Northern America (UN)",
    "Latin America and the Caribbean (UN)",
    "Middle Africa (UN)",
    "Northern Africa (UN)",
    "Northern Africa and Western Asia (UN)",
    "Northern America (UN)",
    "Northern Europe (UN)",
    "South-Eastern Asia (UN)",
    "Southern Africa (UN)",
    "Southern Asia (UN)",
    "Southern Europe (UN)",
    "Sub-Saharan Africa (UN)",
    "Western Africa (UN)",
    "Western Asia (UN)",
    "Western Europe (UN)",
    # Income/Development Groups
    "High-income countries",
    "Upper-middle-income countries",
    "Lower-middle-income countries",
    "Low-income countries",
    "Land-locked Developing Countries (LLDC)",
    "Small Island Developing States (SIDS)",
    # Broad Geographic Regions
    "Africa",
    "Asia",
    "Europe",
    "North America",
    "Oceania",
    "South America",
    "World",
    "Melanesia",
    "Micronesia",
    "Polynesia",
    # Political/Economic Blocs
    "European Union (27)",
]

paths = PathFinder(__file__)

GENDER_MAP = {
    "all": {"id": 1, "name": "All genders"},
    "female": {"id": 2, "name": "Female"},
    "male": {"id": 3, "name": "Male"},
}


POPULATION_YEARS = [1990, 1995, 2000, 2005, 2010, 2015, 2020, 2024]


def create_metadata_json(tb: Table, tb_pop: Table) -> tuple[dict, dict]:
    """Build the metadata JSON and return it alongside lookup dicts."""
    # All entities = union of destinations and origins
    all_entities = sorted(set(tb["country_destination"].unique()) | set(tb["country_origin"].unique()))

    # Build a (country → year → population) lookup in one pass
    pop_pivot = tb_pop[tb_pop["year"].isin(POPULATION_YEARS)].set_index(["country", "year"])["population"].to_dict()

    def _pop_list(entity: str) -> list:
        return [int(pop_pivot[(entity, yr)]) for yr in POPULATION_YEARS]

    entities = [{"id": i + 1, "name": e, "population": _pop_list(e)} for i, e in enumerate(all_entities)]
    entity_to_id = {e: i + 1 for i, e in enumerate(all_entities)}

    genders = [{"id": v["id"], "name": v["name"]} for v in sorted(GENDER_MAP.values(), key=lambda x: x["id"])]

    metadata = {
        "timeRange": {"start": int(tb["year"].min()), "end": int(tb["year"].max())},
        "years": sorted(tb["year"].unique().tolist()),
        "source": "UN DESA, International Migrant Stock (2024)",
        "dimensions": {
            "entities": entities,
            "genders": genders,
        },
    }

    mappings = {
        "entities": all_entities,
        "entity_to_id": entity_to_id,
        "gender_to_id": {g: v["id"] for g, v in GENDER_MAP.items()},
    }

    return metadata, mappings


def create_entity_data_json(tb: Table, entity: str, mappings: dict) -> dict:
    """Build the data JSON for a single entity.

    Returns two sections:
    - "immigrants": rows where country_destination == entity
    - "emigrants": rows where country_origin == entity
    """
    entity_to_id = mappings["entity_to_id"]
    gender_to_id = mappings["gender_to_id"]

    def _encode_rows(rows: Table, other_col: str) -> dict:
        rows = rows.sort_values(["year", "gender", other_col]).reset_index(drop=True)
        return {
            "entities": [entity_to_id[e] for e in rows[other_col]],
            "years": rows["year"].tolist(),
            "genders": [gender_to_id[g] for g in rows["gender"]],
            "values": [None if pd.isna(v) else int(v) for v in rows["migrants"]],
        }

    tb_immigrants = tb[tb["country_destination"] == entity].copy()
    tb_emigrants = tb[tb["country_origin"] == entity].copy()

    return {
        "immigrants": _encode_rows(tb_immigrants, "country_origin"),
        "emigrants": _encode_rows(tb_emigrants, "country_destination"),
    }


def save_and_upload_json(data: dict, filename: str) -> None:
    """Write JSON locally and upload to S3 (unless DRY_RUN)."""
    export_dir = EXPORT_DIR / paths.channel / paths.namespace / paths.version / paths.short_name
    export_dir.mkdir(parents=True, exist_ok=True)

    local_file = export_dir / filename
    s3_path = S3_DATA_DIR / filename

    with open(local_file, "w") as f:
        json.dump(data, f, separators=(",", ":"))

    if DRY_RUN:
        tqdm.write(f"[DRY RUN] Would upload {local_file} to s3://{S3_BUCKET_NAME}/{s3_path}")
        # save locally under temp files on desktop for inspection
        temp_dir = Path.home() / "Desktop" / "temp_migration_flows_json"
        temp_dir.mkdir(exist_ok=True)
        temp_file = temp_dir / filename
        with open(temp_file, "w") as f:
            json.dump(data, f, indent=2)
        tqdm.write(f"[DRY RUN] Also saved a pretty-printed version to {temp_file} for inspection.")
    else:
        s3_utils.upload(f"s3://{S3_BUCKET_NAME}/{str(s3_path)}", local_file, public=True, downloadable=True)


def run() -> None:
    #
    # Load data.
    #
    log.info("Loading migration_stock_flows dataset.")
    ds_garden = paths.load_dataset("migration_stock_flows")
    tb = ds_garden["migrant_stock_dest_origin"].reset_index()

    ds_population = paths.load_dataset("population")
    tb_pop = ds_population["population"].reset_index()

    # filter out regions and World aggregate since they are not included in the visualization
    tb = tb[~tb["country_destination"].isin(REGIONS) & ~tb["country_origin"].isin(REGIONS)]

    # exclude channel islands (null population in population dataset)
    tb = tb[~tb["country_destination"].isin(["Channel Islands"]) & ~tb["country_origin"].isin(["Channel Islands"])]

    #
    # Build metadata + entity file.
    #
    log.info("Creating metadata JSON.")
    metadata, mappings = create_metadata_json(tb, tb_pop)

    total_files = len(mappings["entities"]) + 1
    log.info(f"Creating and {'uploading' if not DRY_RUN else 'dry-running'} {total_files} JSON files.")

    save_and_upload_json(metadata, "migration-stock-flows.metadata.json")

    #
    # Per-entity files.
    #
    entity_to_id = mappings["entity_to_id"]
    for entity in tqdm(mappings["entities"], desc="Processing entities"):
        entity_id = entity_to_id[entity]
        data = create_entity_data_json(tb, entity, mappings)
        save_and_upload_json(data, f"migration-stock-flows.{entity_id}.json")

    log.info(
        f"Done. Created {total_files} files: 1 metadata + {len(mappings['entities'])} entity files. (DRY_RUN={DRY_RUN})"
    )
