"""Script to create a snapshot of dataset 'Food expenditure in United States'.

WARNING: USDA ERS dataset adds a year of data and removes the oldest year. To keep all data, we run this snapshot every year, and keep the previous ones.
So, on the next update:
* Rename the accompanying food_expenditure_since_*.xlsx.dvc file appropriately
* Use that same file name in this script (below).

"""

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
def run(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"usda_ers/{SNAPSHOT_VERSION}/food_expenditure_since_2019.xlsx")
    snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    run()
