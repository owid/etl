"""Script to create a snapshot of dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run(upload: bool = True) -> None:
    """Create a new snapshot."""
    # Init Snapshot object.
    snap = paths.init_snapshot()

    # Save snapshot.
    snap.create_snapshot(upload=upload)
