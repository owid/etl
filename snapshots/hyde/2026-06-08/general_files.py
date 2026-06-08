"""Script to create a snapshot of dataset.

The Yoda data host serves an Anubis bot-challenge page to OWID's default ``owid-etl`` User-Agent (the
one the built-in auto-download uses), so we fetch the file with a plain ``requests`` call (which is
not challenged) and guard against silently storing the challenge HTML instead of the real zip.
"""

import tempfile
from pathlib import Path

import click
import requests
from owid.catalog import Origin

from etl.helpers import PathFinder

# Get paths and naming conventions for current snapshot.
paths = PathFinder(__file__)


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = paths.init_snapshot()

    assert isinstance(snap.metadata.origin, Origin)
    url = snap.metadata.origin.url_download
    assert url is not None, "url_download must be set in the .dvc metadata."

    # Download with a plain requests User-Agent (OWID's default UA is blocked by the Anubis bot wall).
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    assert resp.content[:2] == b"PK", (
        f"Expected a zip file but got {resp.headers.get('content-type')} from {url} "
        "(likely an Anubis bot-challenge page)."
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = Path(tmpdir) / "general_files.zip"
        zip_path.write_bytes(resp.content)

        # Copy the downloaded zip to the snapshot, add to DVC and upload to S3.
        snap.create_snapshot(filename=zip_path, upload=upload)
