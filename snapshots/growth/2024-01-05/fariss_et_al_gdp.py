"""Script to create a snapshot of dataset."""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name
file_list = ["gdp", "gdp_pc", "pop"]


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    """
    Create a snapshot of the three files available in the Fariss et al. dataset.
    GDP, GDP per capita and population.
    """
    for file in file_list:
        # Create a new snapshot.
        snap = Snapshot(f"growth/{SNAPSHOT_VERSION}/fariss_et_al_{file}.rds")

        # Download data from source, add file to DVC and upload to S3.
        snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    main()
