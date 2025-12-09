"""Script to create a snapshot of dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run(upload: bool = True) -> None:
    """Create a new snapshot.

    Args:
        upload: Whether to upload the snapshot to S3.
    """
    # Init snapshot object
    snap = paths.init_snapshot()

    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload)
