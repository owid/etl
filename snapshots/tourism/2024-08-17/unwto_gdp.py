"""Script to create a snapshot of dataset."""

from etl.snapshot import Snapshot


def run(upload: bool = True) -> None:
    """Create a new snapshot.

    Args:
        upload: Whether to upload the snapshot to S3.
    """
    # Create a new snapshot using the script's location.
    snap = Snapshot.from_script(__file__)

    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload)
