"""Script to create a snapshot of dataset 'Excess mortality during the COVID-19 pandemic'."""
from datetime import date
from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
def main(upload: bool) -> None:
    # Data aggregated on all ages
    add_snapshot(f"excess_mortality/{SNAPSHOT_VERSION}/xm_karlinsky_kobak.csv", upload)
    # Data broken down by ages
    add_snapshot(f"excess_mortality/{SNAPSHOT_VERSION}/xm_karlinsky_kobak_ages.csv", upload)


def add_snapshot(uri: str, upload: bool):
    # Load snapshot
    snap = Snapshot(uri)
    # Add date_accessed
    snap.metadata.date_accessed = date.today()
    snap.metadata.save()
    # Download file
    snap.download_from_source()
    # Add to bucket
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
