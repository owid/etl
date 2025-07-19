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
    snap = Snapshot(f"fda/{SNAPSHOT_VERSION}/drugs_at_fda_json.zip")

    # Save snapshot.
    snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    run()
