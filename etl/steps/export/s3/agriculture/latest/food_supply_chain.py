"""S3 JSON export for the food supply chain waterfall visualization.

Reads whichever food supply chain garden dataset is the step's dependency in the DAG (`food_supply_chain_fbs` or
`food_supply_chain_scl`; the DAG line decides which method is published) and writes, in the shape of the other
bespoke-visualization exports (food trade, causes of death):

  * one metadata JSON at `food-supply-chain.metadata.json`: the method, the sources, the year range, the stages of
    the chain in order (with a label and whether the bar adds to or takes from the chain), the units, and the
    entity id-to-name mapping;
  * one JSON per entity at `food-supply-chain.<entity_id>.json`, with the years and, for each unit (energy,
    protein, mass), one array per stage aligned with the years.

Reshaping only; all logic lives in the garden steps. Values are FAO's sign convention: stages listed with
"direction": "out" are magnitudes to subtract along the chain (a negative value there adds back), and the chain
lands exactly on "food".

Output URLs:
    https://owid-public.owid.io/data/food-supply-chain/food-supply-chain.metadata.json
    https://owid-public.owid.io/data/food-supply-chain/food-supply-chain.<entity_id>.json
"""

import json
from pathlib import Path

from owid.catalog import s3_utils
from structlog import get_logger
from tqdm.auto import tqdm

from etl.config import DRY_RUN
from etl.helpers import PathFinder
from etl.paths import EXPORT_DIR

log = get_logger()
paths = PathFinder(__file__)

# Public S3 bucket and prefix.
S3_BUCKET_NAME = "owid-public"
S3_DATA_DIR = Path("data/food-supply-chain")
FILE_SLUG = "food-supply-chain"

# The two garden datasets this step can publish, and how the method is named in the metadata.
METHODS = {
    "food_supply_chain_fbs": "Food Balance Sheets",
    "food_supply_chain_scl": "Supply Utilization Accounts",
}
# The three tables of the garden dataset.
NUTRIENTS = ["energy", "protein", "mass"]
# Stages in chain order, with a display label and whether the bar adds to ("in") or takes from ("out") the chain.
# "food" is the total the chain lands on.
STAGES = [
    ("crop_production", "Crop production", "in"),
    ("imports", "Imports", "in"),
    ("exports", "Exports", "out"),
    ("stock_variation", "Stock change", "out"),
    ("seed", "Seed", "out"),
    ("losses", "Losses in the supply chain", "out"),
    ("other_uses", "Industrial and other non-food uses", "out"),
    ("processing_net", "Processing, net", "out"),
    ("feed", "Animal feed", "out"),
    ("animal_products", "Livestock, dairy, eggs and fish", "in"),
    ("tourist_consumption", "Tourist consumption", "out"),
    ("residuals", "Residuals and balancing", "out"),
    ("food", "Food available to eat", "total"),
]
# Decimal places kept per unit.
NUM_DECIMALS = {"energy": 1, "protein": 2, "mass": 4}


def _save_and_upload(data: dict, filename: str) -> None:
    """Write JSON locally and upload to S3 (skipping the upload under DRY_RUN)."""
    export_dir = EXPORT_DIR / paths.channel / paths.namespace / paths.version / paths.short_name
    export_dir.mkdir(parents=True, exist_ok=True)
    local_file = export_dir / filename
    s3_path = f"s3://{S3_BUCKET_NAME}/{S3_DATA_DIR / filename}"
    with open(local_file, "w") as f:
        json.dump(data, f, separators=(",", ":"))
    if DRY_RUN:
        tqdm.write(f"[DRY RUN] Would upload {local_file} -> {s3_path}")
    else:
        s3_utils.upload(s3_path, local_file, public=True, downloadable=True)


def run() -> None:
    #
    # Load inputs: whichever of the two garden datasets is the dependency.
    #
    dependencies = [d for d in paths.dependencies if d.split("/")[-1] in METHODS]
    assert len(dependencies) == 1, f"Expected exactly one food supply chain garden dependency, found {dependencies}."
    short_name = dependencies[0].split("/")[-1]
    ds = paths.load_dataset(short_name)
    tables = {nutrient: ds.read(nutrient) for nutrient in NUTRIENTS}

    stage_keys = [key for key, _, _ in STAGES]
    for nutrient, tb in tables.items():
        assert set(stage_keys) <= set(tb.columns), (
            f"Table {nutrient!r} lacks stages: {set(stage_keys) - set(tb.columns)}"
        )
    reference = tables["energy"]

    #
    # Metadata: entities get 1-based alphabetical ids, as in the other bespoke exports.
    #
    entities = sorted(set(reference["country"].astype(str)))
    entity_to_id = {name: i + 1 for i, name in enumerate(entities)}
    metadata = {
        "method": METHODS[short_name],
        # Every origin behind the chain (FAO's balances, OWID's population), not only the first.
        "sources": sorted({origin.attribution for origin in reference["food"].metadata.origins if origin.attribution}),
        "timeRange": {"start": int(reference["year"].min()), "end": int(reference["year"].max())},
        "units": {nutrient: tables[nutrient]["food"].metadata.unit for nutrient in NUTRIENTS},
        "stages": [{"key": key, "name": name, "direction": direction} for key, name, direction in STAGES],
        "dimensions": {"entities": [{"id": entity_to_id[name], "name": name} for name in entities]},
    }
    log.info("food_supply_chain.write_metadata", method=METHODS[short_name], n_entities=len(entities))
    _save_and_upload(metadata, f"{FILE_SLUG}.metadata.json")

    #
    # One file per entity: years, then for each unit one array per stage aligned with the years.
    #
    for name in tqdm(entities, desc="food_supply_chain per-entity JSON"):
        years = None
        data = {}
        for nutrient in NUTRIENTS:
            rows = tables[nutrient][tables[nutrient]["country"].astype(str) == name].sort_values("year")
            if years is None:
                years = rows["year"].astype(int).tolist()
            assert rows["year"].astype(int).tolist() == years, f"Tables have different years for {name}."
            data[nutrient] = {
                key: [None if v != v else v for v in rows[key].astype(float).round(NUM_DECIMALS[nutrient])]
                for key in stage_keys
            }
        _save_and_upload({"years": years, **data}, f"{FILE_SLUG}.{entity_to_id[name]}.json")
