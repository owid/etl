"""Script to create a snapshot of dataset."""

from pathlib import Path

import click
import structlog

from etl.download_helpers import DownloadCorrupted
from etl.snapshot import Snapshot

log = structlog.get_logger()

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"who/{SNAPSHOT_VERSION}/vaccination_coverage.xlsx")

    # Download data from source, add file to DVC and upload to S3.
    # NOTE: the source is unreliable, retry if download is corrupted. Sometimes even retrying
    #   does not help, in that case log it as an error, but don't raise it to avoid
    #   breaking autoupdates.
    try:
        snap.create_snapshot(upload=upload, download_retries=4)
    except DownloadCorrupted as e:
        log.error("Download corrupted, try again.", error=str(e))


if __name__ == "__main__":
    main()
