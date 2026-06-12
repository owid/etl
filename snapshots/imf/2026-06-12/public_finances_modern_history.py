"""Script to create a snapshot of dataset.

The file is downloaded directly from the "Full data in Excel" link on
https://www.imf.org/external/datamapper/datasets/FPP (set as `url_download` in the .dvc metadata).
The link's filename carries the release month (e.g. "Public Finances in Modern History Dec 2025.xlsx"),
so on each update copy the fresh link from that page into `url_download`.

NOTE: The IMF site rejects browser-like User-Agents (403) while letting plain, honestly-identified
clients through — the inverse of the usual bot-blocking. The downloader's default UA is browser-like,
so we pass a plain custom UA explicitly.
"""

import click

from etl.helpers import PathFinder

# Get paths and naming conventions for current snapshot.
paths = PathFinder(__file__)


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = paths.init_snapshot()

    # Download data from source (with a plain UA — see module docstring), add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload, user_agent="owid-etl/1.0 (https://ourworldindata.org)")
