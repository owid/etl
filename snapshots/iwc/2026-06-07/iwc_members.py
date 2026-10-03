"""Script to create a snapshot of dataset.

Steps to download the data manually:
  1. Go to https://iwc.int/commission/members
  2. Download the member countries data and save it as a CSV file.
  3. Run: python snapshots/iwc/2026-06-07/iwc_members.py --path-to-file <path>
"""

import click

from etl.helpers import PathFinder

paths = PathFinder(__file__)


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--path-to-file", prompt=True, type=str, help="Path to local data file.")
def run(upload: bool = True, path_to_file: str = "") -> None:
    snap = paths.init_snapshot()
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    run()
