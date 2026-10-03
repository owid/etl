"""
Script to create a snapshot of dataset.
To access the data go here:
- https://www.tropicalmedicine.ox.ac.uk/gram/research/visualisation-app-antibiotic-usage-and-consumption
- Click on the model estimates tab
- Select the desired data slice, so Indicator = Total Antibiotic Consumption, Antibiotic grouping = All Antibiotics
- Download the data

"""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--path-to-file", prompt=True, type=str, help="Path to local data file.")
def main(path_to_file: str, upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"antibiotics/{SNAPSHOT_VERSION}/gram.csv")

    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
