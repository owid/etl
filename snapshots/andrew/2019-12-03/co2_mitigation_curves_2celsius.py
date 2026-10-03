"""Script to create a snapshot of dataset."""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def run(upload: bool) -> None:
    # Initialize a new snapshot.
    snap = Snapshot("andrew/2019-12-03/co2_mitigation_curves_2celsius.csv")

    # Save snapshot.
    snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    run()
