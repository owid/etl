"""Script to create a snapshot of dataset."""

import time
from pathlib import Path

import click
from structlog import get_logger

from etl.snapshot import Snapshot

log = get_logger()

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name
# List of countries
## These are files per country listed in https://github.com/YouGov-Data/covid-19-tracker/tree/master/data
SNAP_NAMES = [
    "country.csv",
    "country_cum.csv",
    "country_100k.csv",
    "country_cum_100k.csv",
    "world.csv",
    "world_cum.csv",
    "world_100k.csv",
    "world_cum_100k.csv",
    "location.csv",
]


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    for name in SNAP_NAMES:
        log.info(name)
        # Create a new snapshot.
        snap = Snapshot(f"covid/{SNAPSHOT_VERSION}/xm_econ_{name}")
        snap.create_snapshot(upload=upload)

        time.sleep(2)


if __name__ == "__main__":
    main()
