"""Script to create a snapshot of dataset."""

import click

from etl.helpers import PathFinder

paths = PathFinder(__file__)


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--path-to-file", prompt=True, type=str, help="Path to local data file.")
def run(path_to_file: str, upload: bool) -> None:
    """Create a new snapshot.

    Args:
        upload: Whether to upload the snapshot to S3.
    """
    snap = paths.init_snapshot()
    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)
