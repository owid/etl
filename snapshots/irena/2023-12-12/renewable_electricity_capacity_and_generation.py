"""Script to create a snapshot of dataset 'Renewable Electricity Capacity and Generation Statistics'."""

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
    # Create a new snapshot.
    snap = Snapshot(f"irena/{SNAPSHOT_VERSION}/renewable_electricity_capacity_and_generation.xlsm")

    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    main()
