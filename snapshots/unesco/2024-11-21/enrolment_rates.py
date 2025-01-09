"""Script to create a snapshot of dataset.
To download the csv file go to https://data.uis.unesco.org/index.aspx?queryid=3813 click on the Data Other Policy Indicators (full dataset) and then export as CSV.
This data should be the most recent version of the OPRI dataset.
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
    snap = Snapshot(f"unesco/{SNAPSHOT_VERSION}/enrolment_rates.csv")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
