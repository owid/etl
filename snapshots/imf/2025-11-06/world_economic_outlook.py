"""
Script to create a snapshot of dataset.

The IMF doesn't allow automatic download of the dataset, so we need to manually download the dataset from the IMF website.
    1. Visit https://data.imf.org/en/datasets/IMF.RES:WEO
    2. Select "Download"
    3. Keep these default options:
        - Download full dataset
        - File format: CSV
        - Data format: Timeseries per row
        - Metadata
    4. Click "Download"
    5. Save the file to this folder.
    6. Run this command on the terminal:
        python snapshots/imf/{version}/world_economic_outlook.py --path-to-file <path-to-file>
    7. Delete the file from the folder.
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
    snap = Snapshot(f"imf/{SNAPSHOT_VERSION}/world_economic_outlook.csv")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
