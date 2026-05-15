"""S3 export step for the FAOSTAT food-trade Sankey viz.

Thin wrapper around the `food_trade` garden step: load that table, rename
columns to the viz-facing CamelCase form, write CSV, upload to S3. All the
data work (rollup, Production join, apparent-supply computation, etc.) lives
in the garden step.

Output schema (matches the TradeRow type in owid-grapher's bespoke food-trade
project — see `bespoke/projects/food-trade/src/data.ts`):
    Exporter            (str)
    Importer            (str)
    Item                (str)
    Value               (float, tonnes)
    ExporterProduction  (float, tonnes; NaN if QCL has no Production figure)
    ImporterSupply      (float, tonnes; NaN when no Production for importer)

Output URL:
    https://owid-public.owid.io/food-trade/trade.preview.csv

The `.preview` suffix is intentional: the viz currently reads a hand-uploaded
`trade.csv` at the same prefix. Writing to a different filename means the
pipeline-produced file can land next to that one without clobbering it, and
the viz developer can flip their `DATA_URL` to `trade.preview.csv` when
ready to switch. The schema is a strict superset of what the viz currently
parses (the four CamelCase columns Exporter, Importer, Item, Value are
identical; ExporterProduction and ImporterSupply are extras).
"""

import tempfile
from pathlib import Path

import pandas as pd
import structlog
from owid.catalog import s3_utils

from etl.config import DRY_RUN
from etl.helpers import PathFinder

log = structlog.get_logger()
paths = PathFinder(__file__)

# Public S3 bucket and prefix. We deliberately write to `trade.preview.csv`
# rather than `trade.csv` so the pipeline output doesn't overwrite the
# hand-uploaded `trade.csv` the viz currently reads. Once the viz developer
# is ready to cut over, either rename this to `trade.csv` here, or have the
# viz point its DATA_URL at the preview file.
S3_BUCKET_NAME = "owid-public"
S3_DATA_DIR = Path("food-trade")
S3_FILENAME = "trade.preview.csv"

# Map garden column names (snake_case) → viz-facing CSV column names (CamelCase).
COLUMN_RENAMES = {
    "exporter": "Exporter",
    "importer": "Importer",
    "item": "Item",
    "value": "Value",
    "exporter_production": "ExporterProduction",
    "importer_supply": "ImporterSupply",
}


def run() -> None:
    #
    # Load inputs.
    #
    ds = paths.load_dataset("food_trade")
    tb = ds.read("food_trade")

    #
    # Process data.
    #
    out = pd.DataFrame(tb).rename(columns=COLUMN_RENAMES)[list(COLUMN_RENAMES.values())]

    #
    # Save outputs.
    #
    with tempfile.TemporaryDirectory() as tmp:
        local_file = Path(tmp) / S3_FILENAME
        out.to_csv(local_file, index=False)
        s3_url = f"s3://{S3_BUCKET_NAME}/{S3_DATA_DIR / S3_FILENAME}"
        size_mb = f"{local_file.stat().st_size / 1e6:.1f}"
        if DRY_RUN:
            log.info("food_trade.dry_run_skip_upload", local=str(local_file), s3=s3_url, size_mb=size_mb)
        else:
            log.info("food_trade.uploading", s3=s3_url, size_mb=size_mb)
            s3_utils.upload(s3_url, local_file, public=True, downloadable=True)
