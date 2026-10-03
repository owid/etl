"""Script to create a snapshot of dataset.

Steps to download the data manually:
  1. Go to https://gfinderdata.impactglobalhealth.org/pages/data-visualisations/allNeglectedDiseases
  2. Click on the "Download All Data" button in the bottom left corner to download the data as an Excel file.
  3. Run: python snapshots/neglected_tropical_diseases/2026-06-25/funding.py --path-to-file <path>
"""

import click

from etl.helpers import PathFinder

paths = PathFinder(__file__)


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--path-to-file", prompt=True, type=str, help="Path to local data file.")
def run(upload: bool = True, path_to_file: str | None = None) -> None:
    snap = paths.init_snapshot()
    snap.create_snapshot(filename=path_to_file, upload=upload)
