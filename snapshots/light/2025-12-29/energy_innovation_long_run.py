"""
Script to create a snapshot of dataset.

The file is provided by the author by email.

To update the snapshot, download the file again and run:
    etls light/2025-12-29/energy_innovation_long_run --path-to-file <PATH_TO_FILE>
"""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run(upload: bool = True, path_to_file: str = None) -> None:
    """Create a new snapshot.

    Args:
        upload: Whether to upload the snapshot to S3.
        path_to_file: Path to local data file.
    """
    # Init Snapshot object
    snap = paths.init_snapshot()

    # Save snapshot from local file.
    snap.create_snapshot(filename=path_to_file, upload=upload)
