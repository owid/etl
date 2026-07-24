"""Script to create a snapshot of dataset.

The data file is provided manually. Run with:
  etls iea/2026-07-20/share_of_households_with_air_conditioning --path-to-file <path>
"""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run(upload: bool = True, path_to_file: str | None = None) -> None:
    snap = paths.init_snapshot()
    snap.create_snapshot(filename=path_to_file, upload=upload)
