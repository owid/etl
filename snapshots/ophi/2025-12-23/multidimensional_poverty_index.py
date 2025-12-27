"""Script to create a snapshot of dataset."""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Define snapshot variants
SNAPSHOT_VARIANTS = ["cme", "hot"]


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    for variant in SNAPSHOT_VARIANTS:
        # Create a new snapshot.
        snap = Snapshot(f"ophi/{SNAPSHOT_VERSION}/multidimensional_poverty_index_{variant}.csv")

        # Download data from source, add file to DVC and upload to S3.
        snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    main()
