"""Script to create a snapshot of dataset.

To obtain the file, you need to log in into the IEA website and download the XLSX file in:
https://www.iea.org/data-and-statistics/data-product/fossil-fuel-subsidies-database#data-sets

Note that creating an account is free, and this dataset is also free of cost.

"""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--path-to-file", prompt=True, type=str, help="Path to local data file.")
def main(path_to_file: str, upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"iea/{SNAPSHOT_VERSION}/fossil_fuel_subsidies.xlsx")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
