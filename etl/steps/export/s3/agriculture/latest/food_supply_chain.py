"""Export the food supply chain (FAOSTAT Food Balance Sheets in kilocalories per person per day) as one JSON file.

Reshaping only: the field names are the columns of the garden table `food_supply_chain`, and the values are the
garden values rounded. The file is written locally and uploaded to the public S3 bucket unless DRY_RUN is set.

Schema:
    {
      "source": "<attribution of the FBS origin>",
      "unit": "kilocalories per person per day",
      "stages": ["crop_production", "imports", ...],          # garden columns, in chain order
      "entities": {
        "<entity name>": {
          "year": [1961, 1962, ...],
          "crop_production": [..., ...],                       # one value per year, null where missing
          ...
        }
      }
    }

Output URL:
    https://owid-public.owid.io/data/food-supply-chain/food-supply-chain.json
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
FILENAME = "food-supply-chain.json"

# Decimal places kept for values (kcal per person per day).
NUM_DECIMALS = 2


def run() -> None:
    #
    # Load inputs.
    #
    ds = paths.load_dataset("food_supply_chain")
    tb = ds.read("food_supply_chain")
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

    data = {
        "source": tb["food"].metadata.origins[0].attribution,
        "unit": tb["food"].metadata.unit,
        "stages": stages,
        "entities": entities,
    }

    #
    # Save outputs.
    #
    export_dir = EXPORT_DIR / paths.channel / paths.namespace / paths.version / paths.short_name
    export_dir.mkdir(parents=True, exist_ok=True)
    local_file = export_dir / FILENAME
    with open(local_file, "w") as f:
        json.dump(data, f, separators=(",", ":"))
    log.info("food_supply_chain.export", n_entities=len(entities), n_stages=len(stages), file=str(local_file))

    s3_path = f"s3://{S3_BUCKET_NAME}/{S3_DATA_DIR / FILENAME}"
    if DRY_RUN:
        log.info(f"[DRY RUN] Would upload {local_file} -> {s3_path}")
    else:
        s3_utils.upload(s3_path, local_file, public=True, downloadable=True)
