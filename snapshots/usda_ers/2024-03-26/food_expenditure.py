"""Script to create a snapshot of dataset 'Food expenditure in United States'."""

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
    snap = Snapshot(f"usda_ers/{SNAPSHOT_VERSION}/food_expenditure.xlsx")
    snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    main()
