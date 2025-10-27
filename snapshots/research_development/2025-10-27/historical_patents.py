"""Script to create a snapshot of dataset."""

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
    path_to_file = "/Users/tunaacisu/Downloads/wipo_ip_historical_data/patents_granted_from_1883_to_1979.csv"
    upload=True
    snap.create_snapshot(filename=path_to_file, upload=upload)
