"""Script to create a snapshot of dataset."""

import gzip
from pathlib import Path

import click
import requests

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Base URL for Eurostat API energy data.
BASE_URL = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/sts_inpp_m?format=TSV&compressed=true"

# Dataset codes to download.
# Producer prices in industry, total - monthly data (from 1976 onwards).
DATASET_CODE = "sts_inpp_m"

# Further API parameters to download each file.
URL_SUFFIX = "?format=TSV&compressed=true"


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"eurostat/{SNAPSHOT_VERSION}/producer_prices_in_industry.gz")

    # Ensure output snapshot folder exists, otherwise create it.
    snap.path.parent.mkdir(exist_ok=True, parents=True)

    # Request the data file for the current dataset.
    response = requests.get(f"{BASE_URL}{DATASET_CODE}{URL_SUFFIX}")

    # Save file as .gz.
    with gzip.open(snap.path, "wt", encoding="utf-8") as f:
        f.write(response.text)

    # Create snapshot and upload to R2.
    snap.create_snapshot(upload=upload, filename=snap.path)


if __name__ == "__main__":
    main()
