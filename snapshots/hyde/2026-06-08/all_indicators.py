"""Script to create a snapshot of dataset.

HYDE 3.5 no longer ships the indicator time series as a single ``all_indicators.zip``. Instead, each
indicator is a separate ``*.txt`` file in the baseline scenario's ``txt/`` directory (``*_c.txt`` for
per-country totals, ``*_r.txt`` for per-IMAGE-region totals). This script lists that directory
(``url_download`` in the .dvc metadata), downloads every ``.txt`` file and bundles them into a single
``all_indicators.zip`` so the downstream meadow step (which extracts the zip and reads the ``*_c.txt``
country files) does not need to change.
"""

import re
import tempfile
import zipfile
from pathlib import Path
from urllib.parse import urljoin

import click
import requests

from etl.helpers import PathFinder

# Get paths and naming conventions for current snapshot.
paths = PathFinder(__file__)


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = paths.init_snapshot()

    # URL of the directory that holds one .txt file per indicator.
    dir_url = snap.metadata.origin.url_download
    assert dir_url is not None, "url_download (the txt/ directory URL) must be set in the .dvc metadata."

    # List the .txt files in the directory (hrefs are bare filenames relative to the directory).
    resp = requests.get(dir_url, timeout=120)
    resp.raise_for_status()
    filenames = sorted(set(re.findall(r'href="([^"?/]+\.txt)"', resp.text)))
    assert filenames, f"No .txt files found at {dir_url}"

    # Download every .txt file and bundle them into a single zip.
    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = Path(tmpdir) / "all_indicators.zip"
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for fname in filenames:
                r = requests.get(urljoin(dir_url, fname), timeout=120)
                r.raise_for_status()
                zf.writestr(fname, r.content)

        # Copy the bundled zip to the snapshot, add to DVC and upload to S3.
        snap.create_snapshot(filename=zip_path, upload=upload)
