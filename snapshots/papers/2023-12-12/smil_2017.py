"""Ingest data from Farmer & Lafond (2016) paper.

The data was sent to Max Roser in 2016 in a private communication.
"""

from pathlib import Path

import click

from etl.snapshot import Snapshot

CURRENT_DIR = Path(__file__).parent
SNAPSHOT_VERSION = CURRENT_DIR.name


# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--path-to-file", prompt=True, type=str, help="Path to local data file.")
def main(path_to_file: str, upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"papers/{SNAPSHOT_VERSION}/smil_2017.csv")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
