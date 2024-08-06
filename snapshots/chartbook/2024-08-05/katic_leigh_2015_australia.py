"""
Script to create a snapshot of dataset.

The file comes from the original paper, available here: https://onlinelibrary.wiley.com/doi/abs/10.1111/roiw.12177.
I use a xlsx file from the data extracted in the past by the Chartbook team. See https://docs.google.com/spreadsheets/d/1_WIBAjDLO7ufWuBFhRFr-GMl6dLgLz47-uF5EvC48ZY/edit?gid=0#gid=0
After creating the file, run
    python snapshots/chartbook/2024-08-05/katic_leigh_2015_australia.py --path-to-file <path-to-file>
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
    snap = Snapshot(f"chartbook/{SNAPSHOT_VERSION}/katic_leigh_2015_australia.xlsx")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
