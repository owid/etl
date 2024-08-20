"""
Script to create a snapshot of dataset.

The data for this snapshot is created manually in a csv file by copying the data for the 50th (median) and the 90th percentile.
This data is available in page 17 (all occupations) here https://www.ausstats.abs.gov.au/ausstats/subscriber.nsf/0/284EA51F2E7BD8F9CA25728F000D10AC/$File/63060_May%202006.pdf.

I save the file in the format:
country,year,indicator,value
Australia,2006,50th percentile (median) (2nd quartile),755.0
Australia,2006,90th percentile,1521.0

The datasets for 2010+ are available as Excel files, so there is no need to manually create the data.
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
    snap = Snapshot(f"abs/{SNAPSHOT_VERSION}/employee_earnings_and_hours_australia_2006.csv")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
