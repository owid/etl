"""Script to create a snapshot of dataset.
You can download the data file manually at http://www.bls.gov/tus/datafiles/atuswho-0323.zip or https://www.bls.gov/tus/data/datafiles-0323.htm and upload them here.Trying an automatic download via script results in a 403 - forbidden error."""

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
    snap = Snapshot(f"atus/{SNAPSHOT_VERSION}/atus_who.zip")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
