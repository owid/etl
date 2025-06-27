"""
HOW TO OBTAIN THE FILE:

    1. Download the files from the reproducibility package (https://reproducibility.worldbank.org/catalog/285):

    2. Find the file `pip_2021_aggregate_202505.dta` in the data/02_processed/ folder.

    Copy that file to this folder and run the script:
    python snapshots/wb/{version}/poverty_aggregates.py --path-to-file snapshots/wb/{version}/pip_2021_aggregate_202505.dta

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
    snap = Snapshot(f"wb/{SNAPSHOT_VERSION}/poverty_aggregates.dta")

    # Save snapshots.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    run()
