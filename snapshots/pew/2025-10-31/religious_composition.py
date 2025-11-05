"""Script to create a snapshot of dataset.

Instructions to download data:
- Register an account in Pew Research: https://www.pewresearch.org/.
- Go to https://www.pewresearch.org/dataset/dataset-of-global-religious-composition-estimates-for-2010-and-2020/
- Click on the left button "Download dataset"
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
