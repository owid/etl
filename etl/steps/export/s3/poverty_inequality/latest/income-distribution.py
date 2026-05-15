"""Export the thousand-bins income distribution data to S3.

This data is then used for our bespoke income distribution chart.

This step uses the thousand_bins_distribution dataset dependency as its basis.

Output:
* https://owid-public.owid.io/data/poverty-inequality/income-distribution.2026.json

Run with DRY_RUN=1 to skip S3 upload and only write the local export file.
"""

import json
from pathlib import Path

from owid.catalog import Table, s3_utils

from etl.config import DRY_RUN
from etl.data_helpers.misc import round_to_sig_figs
from etl.helpers import PathFinder
from etl.paths import EXPORT_DIR

# S3 bucket name and folder where dataset files will be stored.
S3_BUCKET_NAME = "owid-public"
S3_DATA_DIR = Path("data/poverty-inequality")
EXPORT_YEAR = 2026
OUTPUT_FILE = f"income-distribution.{EXPORT_YEAR}.json"

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def create_distribution_json(tb: Table, year: int) -> dict:
    """Create the income distribution JSON structure for a single year."""
    tb = tb.reset_index()

    # For now, we only create a file for the selected year.
    # The output file name includes the year, so we can add other years later.
    tb = tb.loc[tb["year"] == year]

    # Sort by quantile before grouping so each country's avgs list runs from poorest to richest bin.
    tb = tb.sort_values(["country", "year", "quantile"])

    if tb.empty:
        raise ValueError(f"No thousand-bins distribution data found for year {year}.")

    # Four significant figures keep the payload compact without meaningfully reducing precision.
    tb["avg"] = tb["avg"].apply(lambda value: round_to_sig_figs(value, sig_figs=4))

    tb_export = (
        tb.groupby(["country", "year", "region"], as_index=False, observed=True, sort=False)
        .agg(pop=("pop", "sum"), avgs=("avg", list))
        .sort_values(["country", "year"])
        .reset_index(drop=True)
    )
    tb_export["pop"] = tb_export["pop"].round().astype(int)

    if tb_export["country"].duplicated().any():
        duplicated_countries = sorted(tb_export.loc[tb_export["country"].duplicated(), "country"].unique())
        raise ValueError(f"Found duplicate country entries for year {year}: {duplicated_countries}")

    return {
        "year": year,
        "data": {
            row["country"]: {
                "country": row["country"],
                "region": row["region"],
                "totalPopulation": row["pop"],
                "avgs": row["avgs"],
            }
            for row in tb_export.to_dict(orient="records")
        },
    }


def save_and_upload_json(data: dict, filename: str, s3_data_dir: Path) -> None:
    """Save JSON data to local file and upload to S3."""
    # Create export directory using paths.
    export_dir = EXPORT_DIR / paths.channel / paths.namespace / paths.version / paths.short_name
    export_dir.mkdir(parents=True, exist_ok=True)

    # Create full paths.
    local_file = export_dir / filename
    s3_path = s3_data_dir / filename

    # Save locally.
    with open(local_file, "w") as f:
        json.dump(data, f, separators=(",", ":"))

    # Upload to S3.
    if DRY_RUN:
        paths.log.info(f"[DRY RUN] Would upload {local_file} to s3://{S3_BUCKET_NAME}/{s3_path}")
    else:
        s3_utils.upload(f"s3://{S3_BUCKET_NAME}/{str(s3_path)}", local_file, public=True, downloadable=True)


def run() -> None:
    #
    # Load data.
    #
    paths.log.info("Loading thousand_bins_distribution dataset.")
    ds_garden = paths.load_dataset("thousand_bins_distribution")
    tb = ds_garden.read("thousand_bins_distribution", reset_index=False, safe_types=False)

    #
    # Generate JSON file.
    #
    paths.log.info("Creating income distribution JSON file.", year=EXPORT_YEAR)
    data = create_distribution_json(tb, year=EXPORT_YEAR)

    #
    # Save and upload JSON file.
    #
    save_and_upload_json(data, OUTPUT_FILE, S3_DATA_DIR)

    paths.log.info(f"Successfully created income distribution JSON file for {EXPORT_YEAR}.")
