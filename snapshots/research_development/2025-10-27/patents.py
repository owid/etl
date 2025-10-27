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

    path_to_file = "/Users/tunaacisu/Downloads/patent_5 - Patent grants by technology_Total count by filing office_1980_2023.csv"
    upload = True

    # Save snapshot from local file.
    snap.create_snapshot(filename=path_to_file, upload=upload)
