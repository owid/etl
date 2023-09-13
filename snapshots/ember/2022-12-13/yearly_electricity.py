"""Ingest script for Ember's Yearly electricity data.

This dataset seems to be more complete and regularly updated than Ember's Global Electricity Review.

It is unclear whether it is as complete as Ember's European Electricity Review (if not, this data will be merged with
European data).

"""

import pathlib

import click

from etl.snapshot import Snapshot

CURRENT_DIR = pathlib.Path(__file__).parent

SNAPSHOT_VERSION = "2022-12-13"


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload Snapshot",
)
def main(upload: bool) -> None:
    snap = Snapshot(f"ember/{SNAPSHOT_VERSION}/yearly_electricity.csv")
    snap.download_from_source()
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
