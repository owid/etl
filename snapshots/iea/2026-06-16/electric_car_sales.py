"""Script to create a snapshot of dataset.

The data file is provided manually. Run with:
  etls iea/2026-06-16/electric_car_sales --path-to-file <path>

The data is published by the IEA as part of the Global EV Outlook. The EV sales and stocks data
can be explored and downloaded from:
https://www.iea.org/data-and-statistics/data-tools/global-ev-data-explorer
"""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run(upload: bool = True, path_to_file: str | None = None) -> None:
    snap = paths.init_snapshot()
    snap.create_snapshot(filename=path_to_file, upload=upload)
