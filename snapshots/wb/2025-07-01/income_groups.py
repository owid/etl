"""Script to create a snapshot of the World Bank's Income Classification dataset."""

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
    snap = Snapshot(f"wb/{SNAPSHOT_VERSION}/income_groups.xlsx")

    # Download data from source and upload to S3.
    snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    main()
