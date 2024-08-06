"""Script to create a snapshot of dataset."""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Define years to extract
YEARS = [2016, 2018, 2021, 2023]


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    for year in YEARS:
        # Create a new snapshot.
        snap = Snapshot(f"abs/{SNAPSHOT_VERSION}/employee_earnings_and_hours_australia_{year}.xlsx")

        # Download data from source, add file to DVC and upload to S3.
        snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    main()
