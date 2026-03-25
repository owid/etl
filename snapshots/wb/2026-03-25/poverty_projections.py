"""
This data is an update to the estimations in the Poverty, Prosperity and Planet Report 2024.
It uses 2021 prices from the World Bank's Poverty and Inequality Platform (PIP) data published in September 2025.

The file used here was provided by Nishant Yonzan via email on November 7, 2025, and it is not available online.

"""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run(upload: bool = True, path_to_file: str | None = None) -> None:
    # Initialize a new snapshot.
    snap = paths.init_snapshot()

    # Save snapshot.
    snap.create_snapshot(filename=path_to_file, upload=upload)
