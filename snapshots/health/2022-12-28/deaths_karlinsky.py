"""Completion on death reporting is a dataset by Karlinsky.

The dataset is available via the repository https://github.com/akarlinsky/death_registration,
which is updated every month or so. The paper can be found https://www.medrxiv.org/content/10.1101/2021.08.12.21261978v2.
"""

import pathlib

import click

from etl.snapshot import Snapshot

CURRENT_DIR = pathlib.Path(__file__).parent


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
def main(upload: bool) -> None:
    snap = Snapshot("health/2022-12-28/deaths_karlinsky.csv")
    snap.download_from_source()
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
