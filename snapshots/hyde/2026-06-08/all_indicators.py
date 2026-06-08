"""Script to create a snapshot of dataset.

The HYDE 3.5 files are hosted on Utrecht University's Yoda platform, which is protected by an
Anubis proof-of-work bot challenge that blocks automated/programmatic downloads. The file must be
downloaded manually from the dataset landing page in a browser, then passed in via --path-to-file:

    https://public.yoda.uu.nl/geo/UU01/F45D44.html

Usage:
    .venv/bin/etls hyde/2026-06-08/all_indicators --n <path-to-all_indicators.zip>
"""

import click

from etl.helpers import PathFinder

# Get paths and naming conventions for current snapshot.
paths = PathFinder(__file__)


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--path-to-file", prompt=True, type=str, help="Path to local data file.")
def main(upload: bool, path_to_file: str) -> None:
    # Create a new snapshot.
    snap = paths.init_snapshot()

    # Copy local data file to snapshot, add to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)
