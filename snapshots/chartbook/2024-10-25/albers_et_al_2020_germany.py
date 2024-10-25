"""
Script to create a snapshot of dataset.

The data comes from this paper: https://www.econstor.eu/handle/10419/268554
I use a xlsx file from the data extracted in the past by the Chartbook team. See https://docs.google.com/spreadsheets/d/1JUaaUnIRsGlVU_nx_5AfUKd-UG-yW-2O/edit?gid=1866291604#gid=1866291604
After creating the file, run
    python snapshots/chartbook/2024-10-25/albers_et_al_2020_germany.py --path-to-file <path-to-file>
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
    snap = Snapshot(f"chartbook/{SNAPSHOT_VERSION}/albers_et_al_2020_germany.xlsx")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
