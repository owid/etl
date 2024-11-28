"""
Script to create a snapshot of dataset.

The IMF doesn't allow automatic download of the dataset, so we need to manually download the dataset from the IMF website.
    1. Visit https://www.imf.org/en/Publications/SPROLLS/world-economic-outlook-databases
    2. Select the latest version of the data.
    3. Select "Entire dataset"
    4. Select "By Countries" to download the file.
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
    snap = Snapshot(f"imf/{SNAPSHOT_VERSION}/world_economic_outlook.xls")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
