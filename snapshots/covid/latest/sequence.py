"""Script to create a snapshot of dataset."""

from datetime import date, datetime
from pathlib import Path

import click
import requests

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"covid/{SNAPSHOT_VERSION}/sequence.json")

    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload)


def _get_latest_update():
    url = "https://github.com/hodcroftlab/covariants/raw/master/web/public/data/update.json"
    field_name = "lastUpdated"
    date_json = requests.get(url).json()
    if field_name in date_json:
        date_raw = date_json[field_name]
        return datetime.fromisoformat(date_raw).date()
    raise ValueError(f"{field_name} field not found!")


def modify_metadata(snap: Snapshot) -> Snapshot:
    """Modify metadata"""
    # Get access date
    snap.metadata.origin.date_accessed = date.today()  # type: ignore
    # Set publication date
    snap.metadata.origin.publication_date = _get_latest_update()  # type: ignore
    # Save
    snap.metadata.save()
    return snap


if __name__ == "__main__":
    main()
