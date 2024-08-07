"""
Script to create a snapshot of dataset.

This file is extracted besides the current version of SEDLAC because there is relative poverty data for Brasil in the years 1981-1988.

This data was saved by Cameron Appel and Joe Hasell in 2021 in Google Sheets: https://docs.google.com/spreadsheets/d/1VzACYwwpZnZ0jGai487dfEGO3AWdokE-/edit?gid=953143589#gid=953143589

In case the snapshot breaks, update it by downloading the data from the Google Sheets, saving it in this folder and running the script again:

    python snapshots/cedlas/2024-07-31/sedlac_poverty_2018.py --path-to-file "snapshots/cedlas/2024-07-31/SEDLAC 2018 - poverty.xlsx"
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
    snap = Snapshot(f"cedlas/{SNAPSHOT_VERSION}/sedlac_poverty_2018.xlsx")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
