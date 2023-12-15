"""Snapshot of UNEP's report called Global trends in renewable energy investment."""

import pathlib

import click

from etl.snapshot import Snapshot

CURRENT_DIR = pathlib.Path(__file__).parent
SNAPSHOT_VERSION = CURRENT_DIR.name


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
def main(upload: bool) -> None:
    snap = Snapshot(f"unep/{SNAPSHOT_VERSION}/global_trends_in_renewable_energy_investment.pdf")
    snap.download_from_source()
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
