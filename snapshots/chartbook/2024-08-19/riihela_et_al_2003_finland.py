"""
Script to create a snapshot of dataset.

The file comes from the original paper, available online here https://trepo.tuni.fi/handle/10024/65466.
I use a csv file from the data extracted in the past by the Chartbook team. See https://docs.google.com/spreadsheets/d/1ZakjK-hP6s4tLJZCEFjR7NVVqTkb6AXwgpvfpSwsT-I/edit?gid=1888715824#gid=1888715824
After creating the file, run
    python snapshots/chartbook/2024-08-19/riihela_et_al_2003_finland.py --path-to-file <path-to-file>
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
    snap = Snapshot(f"chartbook/{SNAPSHOT_VERSION}/riihela_et_al_2003_finland.csv")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
