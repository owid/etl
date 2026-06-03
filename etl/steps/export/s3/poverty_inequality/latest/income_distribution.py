"""Export the thousand-bins income distribution data to S3.

This data is then used for our bespoke income distribution chart.

This step uses the thousand_bins_distribution dataset dependency as its basis.

Output:
* https://owid-public.owid.io/data/poverty-inequality/income-distribution.<year>.json

Run with DRY_RUN=1 to skip S3 upload and only write the local export file.
"""

import json
from pathlib import Path

import pandas as pd
from owid.catalog import Table, s3_utils
from tqdm.auto import tqdm

from etl.config import DRY_RUN
from etl.data_helpers.misc import round_to_sig_figs
from etl.helpers import PathFinder
from etl.paths import EXPORT_DIR

# S3 bucket name and folder where dataset files will be stored.
S3_BUCKET_NAME = "owid-public"
S3_DATA_DIR = Path("data/poverty-inequality")

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def prepare_distribution_table(tb: Table) -> pd.DataFrame:
    """Prepare one row per country-year for JSON export."""
    tb = tb.reset_index()
    # Sort by quantile before grouping so each country's avgs list runs from poorest to richest bin.
    tb = tb.sort_values(["year", "country", "region", "quantile"]).reset_index(drop=True)
    # Four significant figures keep the payload compact without meaningfully reducing precision.
    tb["avg"] = tb["avg"].apply(lambda value: round_to_sig_figs(value, sig_figs=4))

    tb_export = (
        tb.groupby(["year", "country", "region"], as_index=False, observed=True, sort=False)
        .agg(pop=("pop", "sum"), avgs=("avg", list))
        .sort_values(["year", "country"])
        .reset_index(drop=True)
    )
    tb_export["pop"] = tb_export["pop"].round().astype(int)
    tb_export["year"] = tb_export["year"].astype(int)

    invalid_bins = tb_export[tb_export["avgs"].str.len() != 1000]
    if not invalid_bins.empty:
        invalid_bins = invalid_bins.assign(n_bins=invalid_bins["avgs"].str.len())
        invalid_country_years = sorted(
            f"{row.year}: {row.country} ({row.n_bins} bins)"
            for row in invalid_bins[["year", "country", "n_bins"]].itertuples()
        )
        raise ValueError(f"Expected 1000 income bins for every country-year: {invalid_country_years}")

    duplicated = tb_export[tb_export.duplicated(["year", "country"], keep=False)]
    if not duplicated.empty:
        duplicated_countries = sorted(
            f"{row.year}: {row.country}" for row in duplicated[["year", "country"]].drop_duplicates().itertuples()
        )
        raise ValueError(f"Found duplicate country entries: {duplicated_countries}")

    return tb_export


def create_distribution_json(tb_export: pd.DataFrame, year: int) -> dict:
    """Create the income distribution JSON structure for a single year."""
    tb_year = tb_export.loc[tb_export["year"] == year]

    if tb_year.empty:
        raise ValueError(f"No thousand-bins distribution data found for year {year}.")

    return {
        "year": int(year),
        "data": {
            row["country"]: {
                "country": row["country"],
                "region": row["region"],
                "totalPopulation": row["pop"],
                "avgs": row["avgs"],
            }
            for row in tb_year.to_dict(orient="records")
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
    # Prepare data.
    #
    paths.log.info("Preparing income distribution table.")
    tb_export = prepare_distribution_table(tb)
    years = sorted(tb_export["year"].unique())

    #
    # Generate, save, and upload JSON files.
    #
    paths.log.info(f"Creating {len(years)} income distribution JSON files.")
    for year in tqdm(years, desc="Processing years"):
        data = create_distribution_json(tb_export, year=year)
        save_and_upload_json(data, f"income-distribution.{year}.json", S3_DATA_DIR)

    paths.log.info(f"Successfully created {len(years)} income distribution JSON files.")
