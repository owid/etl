"""Ingest script for Ember's Yearly electricity data.

Ember's recommendation is to use the Yearly electricity data by default (which is more regularly updated than the Global
Electricity Review). However, some data from the European Electricity Review is missing in the current Yearly
electricity data. That is why we currently combine both in the combined_electricity step.

"""

import pathlib

import click

from etl.snapshot import Snapshot

# Version of current snapshot.
SNAPSHOT_VERSION = pathlib.Path(__file__).parent.name


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
