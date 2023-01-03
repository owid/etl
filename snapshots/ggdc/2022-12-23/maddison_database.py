"""This script takes a snapshot of Maddison Database 2010, the last dataset with 1 to 1820 world GDP estimates`."""

import pathlib

import click

from etl.snapshot import Snapshot

CURRENT_DIR = pathlib.Path(__file__).parent


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload snapshot",
)
def main(upload: bool) -> None:
    snap = Snapshot("ggdc/2022-12-23/maddison_database.xlsx")
    snap.download_from_source()
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
