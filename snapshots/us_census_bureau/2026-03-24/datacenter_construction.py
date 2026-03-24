"""Script to create a snapshot of dataset."""

from pathlib import Path

import click
import requests
from owid.datautils.io import df_to_file
from structlog import get_logger

from etl.snapshot import Snapshot

# Initialize log.
log = get_logger()

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# URL for the dataset
URL = "https://www.census.gov/construction/c30/xlsx/privtime.xlsx"


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"us_census_bureau/{SNAPSHOT_VERSION}/datacenter_construction.xlsx")

    log.info("Downloading datacenter construction data from Census Bureau")
    response = requests.get(URL)
    response.raise_for_status()

    with open(snap.path, "wb") as f:
        f.write(response.content)

    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
