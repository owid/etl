"""Script to create a snapshot of dataset."""

from pathlib import Path

import click
import requests

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Eurostat API URL for the isoc_ai_iaiu dataset (generative AI use by individuals).
# NOTE: The API returns already-gzip-compressed content regardless of Content-Encoding header,
# so we save the raw bytes directly to the .gz file.


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def run(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"eurostat/{SNAPSHOT_VERSION}/generative_ai_use.gz")

    # Ensure output snapshot folder exists, otherwise create it.
    snap.path.parent.mkdir(exist_ok=True, parents=True)

    # Request the data file. The API returns pre-compressed gzip content.
    response = requests.get(snap.metadata.origin.url_download)
    response.raise_for_status()

    # Save the raw compressed bytes directly (already gzip-compressed by Eurostat).
    with open(snap.path, "wb") as f:
        f.write(response.content)

    # Create snapshot and upload to R2.
    snap.create_snapshot(upload=upload, filename=snap.path)


if __name__ == "__main__":
    run()
