"""Script to create a snapshot of dataset.

The original 2020-12-19 vintage was previously hosted via walden. This script preserves
the same feather file by downloading it directly from the walden bucket.
"""

from pathlib import Path

import click
import requests

from etl.snapshot import Snapshot

SNAPSHOT_VERSION = Path(__file__).parent.name
WALDEN_URL = "https://walden.owid.io/ihme_gbd/2020-12-19/child_mortality.feather"


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def run(upload: bool) -> None:
    snap = Snapshot(f"ihme_gbd/{SNAPSHOT_VERSION}/child_mortality.feather")
    snap.path.parent.mkdir(exist_ok=True, parents=True)

    response = requests.get(WALDEN_URL)
    response.raise_for_status()
    snap.path.write_bytes(response.content)

    snap.create_snapshot(upload=upload, filename=snap.path)
