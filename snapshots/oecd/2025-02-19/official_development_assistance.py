"""
Script to create a snapshot of dataset.

All the snapshots are obtained from OECD Data Explorer links, and from the full dataset the platform provides.

"""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Define the list of OECD datasets to upload, and the file format.
DATASETS = {"dac1": "zip", "dac2a": "zip", "dac5": "zip", "crs": "zip"}


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    for dataset, extension in DATASETS.items():
        # Create a new snapshot.
        snap = Snapshot(f"oecd/{SNAPSHOT_VERSION}/official_development_assistance_{dataset}.{extension}")

        # Download data from source, add file to DVC and upload to S3.
        snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    main()
