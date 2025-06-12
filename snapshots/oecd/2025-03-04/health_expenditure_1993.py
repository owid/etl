"""
Script to create a snapshot of dataset.

The data comes from the OECD health systems book from 1993 (https://search.worldcat.org/title/868237125).

There is no link of the book available online, but we have this folder in Google Drive:
    https://drive.google.com/drive/u/0/folders/1DKkNqTjN3Qhod1GSMGItJjhYtY913CwP

In the folder you can find a scan of the relevant tables, and an Excel file with the data extracted from the scan. We are using that file, available in the Manipulation folder.

Download and copy to this directory and run:
    python snapshots/oecd/{version}/health_expenditure_1993.py --path_to_file {path_to_file}

"""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--path-to-file", "-f", prompt=True, type=str, help="Path to local data file.")
def main(path_to_file: str, upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"oecd/{SNAPSHOT_VERSION}/health_expenditure_1993.xlsx")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
