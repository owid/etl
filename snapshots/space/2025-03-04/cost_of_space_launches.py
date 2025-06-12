"""Script to create a snapshot of dataset."""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Initialize a new snapshot.
    snap = Snapshot(f"space/{SNAPSHOT_VERSION}/cost_of_space_launches.csv")

    # Create the snapshot.
    snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    main()
