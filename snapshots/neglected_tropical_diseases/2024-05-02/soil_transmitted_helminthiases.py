"""Script to create a snapshot of dataset."""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"neglected_tropical_diseases/{SNAPSHOT_VERSION}/soil_transmitted_helminthiases.xlsx")

    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    main()