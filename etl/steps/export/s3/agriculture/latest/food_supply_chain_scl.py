"""Export the food supply chain (Supply Utilization Accounts method, in kilocalories per person per day) as one JSON file.

Reshaping only: the field names are the columns of the garden table `food_supply_chain_scl`, and the values are the
garden values rounded. The file is written locally and uploaded to the public S3 bucket unless DRY_RUN is set.

Schema:
    {
      "source": "<attribution of the FAO origin>",
      "stages": ["crop_production", "imports", ...],          # garden columns, in chain order
      "nutrients": {
        "energy":  {"unit": "kilocalories per person per day", "entities": {"<entity>": {"year": [...], "crop_production": [...], ...}}},
        "protein": {"unit": "grams of protein per person per day", "entities": {...}},
        "mass":    {"unit": "kilograms per person per day", "entities": {...}}
      }
    }

Output URL:
    https://owid-public.owid.io/data/food-supply-chain/food-supply-chain-scl.json
"""

import json
from pathlib import Path

from owid.catalog import s3_utils
from structlog import get_logger

from etl.config import DRY_RUN
from etl.helpers import PathFinder
from etl.paths import EXPORT_DIR

log = get_logger()
paths = PathFinder(__file__)

# Public S3 bucket and prefix.
S3_BUCKET_NAME = "owid-public"
S3_DATA_DIR = Path("data/food-supply-chain")
FILENAME = "food-supply-chain-scl.json"

# Decimal places kept for values (kcal, grams of protein, or kilograms per person per day).
NUM_DECIMALS = 2


def run() -> None:
    #
    # Load inputs.
    #
    ds = paths.load_dataset("food_supply_chain_scl")
    nutrients = {}
    stages = None
    for nutrient in ["energy", "protein", "mass"]:
        tb = ds.read(nutrient)
        stages = [column for column in tb.columns if column not in ["country", "year"]]

        #
        # Reshape.
        #
        entities = {}
        for country, rows in tb.sort_values(["country", "year"]).groupby("country", observed=True, sort=True):
            entity = {"year": rows["year"].astype(int).tolist()}
            for stage in stages:
                values = rows[stage].astype(float).round(NUM_DECIMALS)
                entity[stage] = [None if v != v else v for v in values]
            entities[str(country)] = entity
        nutrients[nutrient] = {"unit": tb["food"].metadata.unit, "entities": entities}
        source = tb["food"].metadata.origins[0].attribution

    data = {"source": source, "stages": stages, "nutrients": nutrients}

    #
    # Save outputs.
    #
    export_dir = EXPORT_DIR / paths.channel / paths.namespace / paths.version / paths.short_name
    export_dir.mkdir(parents=True, exist_ok=True)
    local_file = export_dir / FILENAME
    with open(local_file, "w") as f:
        json.dump(data, f, separators=(",", ":"))
    log.info(
        "food_supply_chain_scl.export",
        n_entities=len(entities),
        n_stages=len(stages),
        n_nutrients=len(nutrients),
        file=str(local_file),
    )

    s3_path = f"s3://{S3_BUCKET_NAME}/{S3_DATA_DIR / FILENAME}"
    if DRY_RUN:
        log.info(f"[DRY RUN] Would upload {local_file} -> {s3_path}")
    else:
        s3_utils.upload(s3_path, local_file, public=True, downloadable=True)
