"""Script to create a snapshot of dataset.

The data file (the authors' replication workbook) is provided manually. Run with:
  etls economics/2026-07-28/wealth_france --path-to-file <path>
"""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run(upload: bool = True, path_to_file: str | None = None) -> None:
    snap = paths.init_snapshot()
    snap.create_snapshot(filename=path_to_file, upload=upload)
