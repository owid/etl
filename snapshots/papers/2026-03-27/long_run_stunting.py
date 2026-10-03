"""Script to create a snapshot of dataset.

Steps to download the data manually:
  1. Go to https://gh.bmj.com/content/11/2/e018607
  2. Download the supplementary data file and save it locally.
  3. Run: python snapshots/papers/2026-03-27/long_run_stunting.py --path-to-file <path>
"""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run(upload: bool = True, path_to_file: str | None = None) -> None:
    """Create a new snapshot.

    Args:
        upload: Whether to upload the snapshot to S3.
        path_to_file: Path to local data file.
    """
    snap = paths.init_snapshot()
    snap.create_snapshot(filename=path_to_file, upload=upload)
