"""Script to create a snapshot of dataset.

Data must be downloaded from here: https://www.ethnologue.com/codes/Language_Code_Data_20250221.zip

Available at: https://www.ethnologue.com/codes/
"""

from pathlib import Path

import click
from fastapi import requests

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--path-to-file", prompt=True, type=str, help="Path to local data file.")
def main(upload: bool, path_to_file: str) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"language/{SNAPSHOT_VERSION}/ethnologue.zip")

    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload, filename=path_to_file)


if __name__ == "__main__":
    main()
