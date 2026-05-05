"""Script to create a snapshot of the RAM Legacy fish stocks dataset.

The CSV is mirrored on the (now-archived) owid-datasets repository. Source data
is the RAM Legacy Stock Assessment Database v4.44 (2018-12-22).
"""

from pathlib import Path

import click

from etl.snapshot import Snapshot

SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    snap = Snapshot(f"biodiversity/{SNAPSHOT_VERSION}/fish_stocks.csv")
    snap.download_from_source()
    snap.dvc_add(upload=upload)
