"""
Script to create a snapshot of dataset.

This file was provided by Christopher Fariss to update an error in the dataset.

Run
    python snapshots/harvard/2024-11-26/global_military_spending_dataset_burden.py --path-to-file <path-to-file>


"""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"harvard/{SNAPSHOT_VERSION}/global_military_spending_dataset_burden.rds")

    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    main()
