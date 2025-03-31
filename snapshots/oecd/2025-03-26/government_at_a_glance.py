"""Script to create a snapshot of dataset."""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Define snapshot names
SNAPSHOT_NAMES = [
    "govt_glance_public_finance",
    "govt_glance_size_public_procurement",
    "govt_glance_public_finance_economic_transaction",
]


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def run(upload: bool) -> None:
    for snapshot_name in SNAPSHOT_NAMES:
        # Initialize a new snapshot.
        snap = Snapshot(f"oecd/{SNAPSHOT_VERSION}/{snapshot_name}.csv")

        # Save snapshot.
        snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    run()
