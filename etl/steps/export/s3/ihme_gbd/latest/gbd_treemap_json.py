"""Export step that generates JSON files for the IHME GBD treemap visualization.

This step combines the GBD treemap datasets and generates:
* A metadata JSON file with categories, dimensions, and time ranges
* Individual data JSON files per entity

Outputs:
* Files are uploaded to S3 and made publicly available at:
  * https://owid-public.owid.io/data/gbd/causes-of-death.metadata.json
  * https://owid-public.owid.io/data/gbd/causes-of-death.<entityId>.json

"""

import json
from pathlib import Path

from owid.catalog import Table, s3_utils
from owid.catalog import processing as pr
from structlog import get_logger
from tqdm.auto import tqdm

from etl.config import DRY_RUN
from etl.helpers import PathFinder
from etl.paths import EXPORT_DIR

# Initialize logger.
log = get_logger()

# S3 bucket name and folder where dataset files will be stored.
S3_BUCKET_NAME = "owid-public"
S3_DATA_DIR = Path("data/gbd")

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def create_metadata_json(tb_filtered: Table) -> tuple[dict, dict]:
    """Create the metadata JSON structure."""
    # Get unique values for mappings
    countries = sorted(tb_filtered["country"].unique())
    causes = sorted(tb_filtered["cause"].unique())
    categories = sorted(tb_filtered["broad_cause"].unique())

    # Create age group mapping with proper display names
    age_group_map = {
        "All ages": {"id": 1, "name": "All ages"},
        "<5 years": {"id": 2, "name": "Children under 5"},
        "5-14 years": {"id": 3, "name": "Children aged 5 to 14"},
        "15-49 years": {"id": 4, "name": "Adults aged 15 to 49"},
        "50-69 years": {"id": 5, "name": "Adults aged 50 to 69"},
        "70+ years": {"id": 6, "name": "Adults aged 70+"},
    }

    # Create entities list
    entities = [{"id": i + 1, "name": country} for i, country in enumerate(countries)]

    # Create age groups list
    age_groups = [{"id": v["id"], "name": v["name"]} for v in sorted(age_group_map.values(), key=lambda x: x["id"])]

    # Create sex mapping
    sex_map = {
        "Both": {"id": 1, "name": "Both sexes"},
        "Female": {"id": 2, "name": "Female"},
        "Male": {"id": 3, "name": "Male"},
    }

    # Create sex list
    sex_list = [{"id": v["id"], "name": v["name"]} for v in sorted(sex_map.values(), key=lambda x: x["id"])]

    # Create categories list
    categories_list = [{"id": i + 1, "name": cat} for i, cat in enumerate(categories)]
    category_name_to_id = {cat: i + 1 for i, cat in enumerate(categories)}

    # Optional descriptions for specific causes
    cause_descriptions = {
        "Heart diseases": "Heart attacks, strokes, and other cardiovascular diseases",
        "Chronic respiratory diseases": "COPD, Asthma, and others",
        "Neurological diseases": "Alzheimer's disease, Parkinson's disease, epilepsy, and others",
        "Digestive diseases": "Cirrhosis and others",
        "Respiratory infections": "Pneumonia, influenza, COVID-19 and others",
        "Neonatal deaths": "Babies who died in the first 28 days of life",
    }

    # Create variables list with age group associations
    variables = []
    for i, cause in enumerate(causes):
        # Find which age groups this cause appears in
        age_groups_for_cause = tb_filtered[tb_filtered["cause"] == cause]["age"].unique()
        age_group_ids = [age_group_map[age]["id"] for age in age_groups_for_cause if age in age_group_map]

        # Get category
        broad_cause = tb_filtered[tb_filtered["cause"] == cause]["broad_cause"].iloc[0]
        category_id = category_name_to_id[broad_cause]

        # Build variable entry
        variable_entry = {
            "id": i + 1,
            "name": cause,
            "ageGroup": sorted(age_group_ids),
        }

        # Only add description if it exists for this cause
        if cause in cause_descriptions:
            variable_entry["description"] = cause_descriptions[cause]

        variable_entry["category"] = category_id

        variables.append(variable_entry)

    # Create metadata JSON
    metadata = {
        "timeRange": {"start": int(tb_filtered["year"].min()), "end": int(tb_filtered["year"].max())},
        "source": "IHME, Global Burden of Disease (2025)",
        "categories": categories_list,
        "dimensions": {"entities": entities, "ageGroups": age_groups, "sexes": sex_list, "variables": variables},
    }

    return metadata, {
        "countries": countries,
        "causes": causes,
        "age_group_map": age_group_map,
        "sex_map": sex_map,
    }


def create_entity_data_json(tb_filtered: Table, country: str, mappings: dict) -> dict:
    """Create data JSON for a specific entity."""
    country_to_id = {country: i + 1 for i, country in enumerate(mappings["countries"])}
    cause_to_id = {cause: i + 1 for i, cause in enumerate(mappings["causes"])}
    age_to_id = {age: mappings["age_group_map"][age]["id"] for age in mappings["age_group_map"].keys()}
    sex_to_id = {sex: mappings["sex_map"][sex]["id"] for sex in mappings["sex_map"].keys()}

    # Filter data for this entity
    tb_entity = tb_filtered[tb_filtered["country"] == country].copy()

    # Sort data for consistent output
    tb_sorted = tb_entity.sort_values(["year", "sex", "age", "cause"]).reset_index(drop=True)

    # Create data JSON for this entity
    data = {
        "values": tb_sorted["value"].tolist(),
        "variables": [cause_to_id[c] for c in tb_sorted["cause"]],
        "years": tb_sorted["year"].tolist(),
        "ageGroups": [age_to_id[a] for a in tb_sorted["age"]],
        "sexes": [sex_to_id[s] for s in tb_sorted["sex"]],
    }

    return data


def save_and_upload_json(data: dict, filename: str, s3_data_dir: Path) -> None:
    """Save JSON data to local file and upload to S3.

    Args:
        data: Dictionary to save as JSON
        filename: Name of the file (e.g., "causes-of-death.1.json")
        s3_data_dir: S3 directory path (within bucket)
    """
    # Create export directory using paths
    export_dir = EXPORT_DIR / paths.channel / paths.namespace / paths.version / paths.short_name
    export_dir.mkdir(parents=True, exist_ok=True)

    # Create full paths
    local_file = export_dir / filename
    s3_path = s3_data_dir / filename

    # Save locally
    with open(local_file, "w") as f:
        json.dump(data, f, indent=2)

    # Upload to S3
    if DRY_RUN:
        tqdm.write(f"[DRY RUN] Would upload {local_file} to s3://{S3_BUCKET_NAME}/{s3_path}")
    else:
        s3_utils.upload(f"s3://{S3_BUCKET_NAME}/{str(s3_path)}", local_file, public=True, downloadable=True)


def run() -> None:
    #
    # Load data.
    #
    log.info("Loading GBD treemap datasets.")
    ds_garden = paths.load_dataset("gbd_treemap")
    ds_garden_child = paths.load_dataset("gbd_child_treemap")

    tb = ds_garden["gbd_treemap"].reset_index()
    tb_child = ds_garden_child["gbd_child_treemap"].reset_index()
    tb = pr.concat([tb, tb_child], ignore_index=True)

    # Filter data for Number metric only (keep all sexes)
    tb_filtered = tb.loc[tb["metric"] == "Number"].copy()

    tb_filtered.set_index(["country", "year", "age", "sex", "broad_cause", "cause", "metric"], verify_integrity=True)

    #
    # Generate JSON files.
    #
    log.info("Creating metadata and data JSON files.")

    # Create metadata
    metadata, mappings = create_metadata_json(tb_filtered)

    #
    # Generate and upload JSON files.
    #
    log.info(f"Creating and uploading {len(mappings['countries']) + 1} JSON files.")

    # Save and upload metadata file
    save_and_upload_json(metadata, "causes-of-death.metadata.json", S3_DATA_DIR)

    # Save and upload entity data files
    country_to_id = {country: i + 1 for i, country in enumerate(mappings["countries"])}
    for country in tqdm(mappings["countries"], desc="Processing entities"):
        entity_id = country_to_id[country]
        data = create_entity_data_json(tb_filtered, country, mappings)
        save_and_upload_json(data, f"causes-of-death.{entity_id}.json", S3_DATA_DIR)

    log.info(
        f"""Successfully created and uploaded {len(mappings['countries']) + 1} files:
        - 1 metadata file
        - {len(mappings['countries'])} entity data files
        - {len(metadata['dimensions']['variables'])} variables
        - {len(metadata['dimensions']['ageGroups'])} age groups
        - {len(metadata['dimensions']['sexes'])} sexes"""
    )
