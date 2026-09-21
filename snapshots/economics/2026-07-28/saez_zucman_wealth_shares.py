"""Script to create a snapshot of dataset.

The data file is the replication workbook for the paper "The Rise of Income and Wealth
Inequality in America: Evidence from Distributional Macroeconomic Accounts" (Saez and
Zucman, 2020), available at https://www.aeaweb.org/articles?id=10.1257/jep.34.4.3.

Run with:
  etls economics/2026-07-28/saez_zucman_wealth_shares --path-to-file <path>
"""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run(upload: bool = True, path_to_file: str | None = None) -> None:
    snap = paths.init_snapshot()
    snap.create_snapshot(filename=path_to_file, upload=upload)
