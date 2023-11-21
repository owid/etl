"""
Script to create a snapshot of dataset Worldwide Bureaucracy Indicators.

Instructions:
1.  Download the dataset from the link below.
    https://datacatalog.worldbank.org/dataset/worldwide-bureaucracy-indicators
2.  Unzip the file and place the csv file in the data folder.
3.  Run the script with the following command:
    python snapshots/wb/{date}/worldwide_bureaucracy_indicators.py --path-to-file <relative-path-to-file>
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
    snap = Snapshot(f"wb/{SNAPSHOT_VERSION}/worldwide_bureaucracy_indicators.csv")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
