"""Script to create a snapshot of dataset - upload from local file emailed by David Gibbs at GFW
Option to get data from Google Earth Engine is in 2024-07-10 version of this script
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
    snap = Snapshot(f"forests/{SNAPSHOT_VERSION}/dominant_driver.xlsx")

    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
