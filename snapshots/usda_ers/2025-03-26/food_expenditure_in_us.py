"""Script to create a snapshot of dataset 'Food expenditure in United States'.

In case you want to manually inspect the original files they can be found in
https://www.ers.usda.gov/data-products/food-expenditure-series

* "National Food Expenditure Series" -> "Normalized food expenditures by all purchasers and household final users"
* "Archived Food Expenditure Tables" -> "Normalized food expenditures by final purchasers and users, from previously-published estimates"

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
    for file_name in ["food_expenditure_in_us_archive.xlsx", "food_expenditure_in_us.xlsx"]:
        # Create a new snapshot.
        snap = Snapshot(f"usda_ers/{SNAPSHOT_VERSION}/{file_name}")
        snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    run()
