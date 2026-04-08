"""Script to create a snapshot of dataset."""

import click

from etl.helpers import PathFinder

paths = PathFinder(__file__)


@click.command()
@click.option("--path-to-file", prompt=True, type=str, help="Path to local data file.")
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def run(path_to_file: str, upload: bool = True) -> None:
    """Create a new snapshot.

    Args:
        upload: Whether to upload the snapshot to S3.
    """
    # Init Snapshot object
    snap = paths.init_snapshot()

    # Save snapshot.
    snap.create_snapshot(filename=path_to_file, upload=upload)
