"""Script to create a snapshot of dataset.

To fetch the data and create the snapshot:
* Go to https://www.transplant-observatory.org/export-database/
* Select:
  * "Export all questions?": "YES"
  * "Geographic area": "All countries"
  * "From Year": Select earliest (2000 seems to be the minimum available).
  * "To Year": Select latest.
* Click on "Download".
* Run this script with the --path-to-file argument.

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
    snap = Snapshot(f"health/{SNAPSHOT_VERSION}/organ_donation_and_transplantation.xlsx")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
