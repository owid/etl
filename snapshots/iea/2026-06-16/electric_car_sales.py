"""Script to create a snapshot of dataset.

The data is published by the IEA as part of the Global EV Outlook. The EV sales and stocks data
can be explored and downloaded from:
https://www.iea.org/data-and-statistics/data-tools/global-ev-data-explorer

"""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--path-to-file", prompt=True, type=str, help="Path to local data file.")
def run(path_to_file: str, upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"iea/{SNAPSHOT_VERSION}/electric_car_sales.csv")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    run()
