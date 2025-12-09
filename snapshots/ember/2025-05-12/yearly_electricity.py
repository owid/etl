"""Ingest script for Ember's Yearly Electricity Data and Yearly Electricity Data Europe.

Ember's recommendation is to use the yearly electricity data by default (which is more regularly updated than the Global
and European Electricity Reviews).

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
def run(upload: bool) -> None:
    for file in ["yearly_electricity__global", "yearly_electricity__europe"]:
        snap = Snapshot(f"ember/{SNAPSHOT_VERSION}/{file}.csv")
        snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    run()
