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
    # NOTE: the Yoda host serves an Anubis bot-challenge to browser-like User-Agents; the download
    # helper detects this and retries with a plain UA, so the standard auto-download works here.
    snap.create_snapshot(upload=upload)
