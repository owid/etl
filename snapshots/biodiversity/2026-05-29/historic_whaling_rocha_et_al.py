"""Script to create a snapshot of dataset.

Data is extracted from paper with claude pdf extraction:
  1. Go to https://spo.nmfs.noaa.gov/content/emptying-oceans-summary-industrial-whaling-catches-20th-century
  2. Extract tables on page 4, 6, 7, 8, 9 as xlsx file.
"""

import click

from etl.helpers import PathFinder

paths = PathFinder(__file__)


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--path-to-file", prompt=True, type=str, help="Path to local data file.")
def run(upload: bool = True, path_to_file: str | None = None) -> None:
    """Create a new snapshot.

    Args:
        upload: Whether to upload the snapshot to S3.
        path_to_file: Path to local data file.
    """
    snap = paths.init_snapshot()
    snap.create_snapshot(filename=path_to_file, upload=upload)
