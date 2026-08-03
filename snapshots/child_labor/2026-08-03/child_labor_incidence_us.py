"""Script to create a snapshot of dataset.

The data file (manually transcribed from the paper) is provided manually. Run with:
  etls child_labor/2026-08-03/child_labor_incidence_us --path-to-file <path>
"""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run(upload: bool = True, path_to_file: str | None = None) -> None:
    snap = paths.init_snapshot()
    snap.create_snapshot(filename=path_to_file, upload=upload)
