"""
Script to create a snapshot of dataset.
Follow the instructions to create a new snapshot in pip_api.py.
"""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run(upload: bool = True, path_to_file: str | None = None) -> None:
    # Create a new snapshot.
    snap = paths.init_snapshot()

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)
