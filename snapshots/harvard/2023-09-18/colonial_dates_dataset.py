"""
Script to create a snapshot of the Colonial Dates dataset.
Steps:
1. Download the zip file from https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/28201
2. Unzip the file and upload the dyads file (COLDAT_dyads.csv) by running:
    python *this code relative path* --path-to-file *path to dyads file*

I am not using the other file (COLDAT-colonies.csv), because it's only a wide version of the same data.
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
    snap = Snapshot(f"harvard/{SNAPSHOT_VERSION}/colonial_dates_dataset.csv")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
