"""Script to create a snapshot of dataset."""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Define years to extract and the format of the file name.
YEARS = {
    2010: "xls",
    2012: "xls",
    2014: "xls",
    2016: "xls",
    2018: "xlsx",
    2021: "xlsx",
    2023: "xlsx",
}


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    for year, format in YEARS.items():
        # Create a new snapshot.
        snap = Snapshot(f"abs/{SNAPSHOT_VERSION}/employee_earnings_and_hours_australia_{year}.{format}")

        # Download data from source, add file to DVC and upload to S3.
        snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    main()
