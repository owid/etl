"""Script to create a snapshot of dataset."""

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
    upload = True
    snap = paths.init_snapshot()
    snap.create_snapshot(filename=path_to_file, upload=upload)
