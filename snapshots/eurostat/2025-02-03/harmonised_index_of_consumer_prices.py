"""Script to create a snapshot of dataset."""

import gzip
from pathlib import Path

import click
import requests

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Base URL for Eurostat API energy data.
BASE_URL = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/prc_hicp_midx?format=TSV&compressed=true"

# Dataset codes to download.
# HICP - monthly data (index) (from 1996 onwards).
DATASET_CODE = "prc_hicp_midx"

# Further API parameters to download each file.
# NOTE: In theory, there is a startPeriod parameter, but when adding "startPeriod=2015-01", the API returns an error:
# <faultstring>EXTRACTION_TOO_BIG: The requested extraction is too big, estimated 7644780 rows, max authorised is 5000000, please change your filters to reduce the extraction size</faultstring>
# which is a bit contradictory, given that I'm trying to reduce the number of rows.
# So, for now, I will download all years (~63MB), but adding compression (~15MB).
URL_SUFFIX = "?format=TSV&compressed=true"


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"eurostat/{SNAPSHOT_VERSION}/harmonised_index_of_consumer_prices.gz")

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
