"""Script to create a snapshot of dataset."""

import click

from etl.helpers import PathFinder

# Get paths and naming conventions for current snapshot.
paths = PathFinder(__file__)


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = paths.init_snapshot()

    # Download data from source (url_download), add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload)
