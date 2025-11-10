"""
This data is an update to the estimations in the Poverty, Prosperity and Planet Report 2024.
It uses 2021 prices from the World Bank's Poverty and Inequality Platform (PIP) data published in September 2025.

The file used here was provided by Nishant Yonzan via email on November 7, 2025, and it is not available online.

"""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--path-to-file", "-f", prompt=True, type=str, help="Path to local data file.")
def run(path_to_file: str, upload: bool) -> None:
    # Initialize a new snapshot.
    snap = Snapshot(f"wb/{SNAPSHOT_VERSION}/poverty_projections.dta")

    # Save snapshots.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    run()
