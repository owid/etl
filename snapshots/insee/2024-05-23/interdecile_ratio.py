"""
Script to create a snapshot of dataset.

To download the dataset
    1. Click on the Télécharger button tab.
    2. Select the oldest Date de début and the latest Date de fin.
    3. Click on the Télécharger (csv) button.
    4. Extract the valeurs_annuelles.csv file from the downloaded zip file.
    5. Run the script with the path to the extracted file.
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
    snap = Snapshot(f"insee/{SNAPSHOT_VERSION}/interdecile_ratio.csv")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
