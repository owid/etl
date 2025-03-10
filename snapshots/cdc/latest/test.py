"""Script to create a snapshot of dataset."""

import datetime as dt
from pathlib import Path

import click
import pandas as pd

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"cdc/{SNAPSHOT_VERSION}/test.json")
    date = get_date_of_update()
    # Download data from source, add file to DVC and upload to S3.
    snap = modify_metadata(snap, date)
    # Add the file to DVC and optionally upload it to S3, based on the `upload` parameter.
    # snap.dvc_add(upload=upload)
    snap.dvc_add(upload=upload)


def modify_metadata(snap: Snapshot, date: str) -> Snapshot:
    snap.metadata.origin.date_published = date  # type: ignore
    snap.metadata.origin.date_accessed = dt.date.today()  # type: ignore
    snap.metadata.save()
    return snap


def get_date_of_update() -> str:
    """
    Get the date of the latest update for yearly measles from the CDC website - https://www.cdc.gov/measles/data-research/
    """
    df = pd.read_csv("https://raw.githubusercontent.com/spoonerf/test/refs/heads/main/test_cases.csv")
    date = df["date"][0]
    return date


if __name__ == "__main__":
    main()
