"""Script to create a snapshot of dataset. The data was provided to us by the Dynabench team and can be accessed internally here:
https://docs.google.com/spreadsheets/d/1L-TJl3_iWuYSseXFOuIJCvR9yQXkMg9UD8pcIARRpMc/edit#gid=0
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
    snap = Snapshot(f"artificial_intelligence/{SNAPSHOT_VERSION}/dynabench.xlsx")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
