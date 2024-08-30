"""
Script to create a snapshot of dataset.

The file is an extraction of table 7.A2 in chapter 7, available here: https://drive.google.com/file/d/1vuJ3gaEIyA-EQsoh-cAOZnWvWE3s-N19/view.
After creating the file, run
    python snapshots/chartbook/2024-08-19/roine_waldenstrom_2015.py --path-to-file <path-to-file>
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
    snap = Snapshot(f"chartbook/{SNAPSHOT_VERSION}/roine_waldenstrom_2015.csv")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
