"""Script to create a snapshot of dataset.

The data is a manual transcription of the benchmark estimates of the share of the labor
force employed in agriculture reported by Broadberry and Gardner (2013), originally
transcribed for https://github.com/owid/notebooks/tree/main/HannahRitchie/employment-agriculture.

To create the snapshot, run:
    etls papers/2026-07-02/broadberry_gardner --path-to-file <path>
"""

import click

from etl.helpers import PathFinder

paths = PathFinder(__file__)


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--path-to-file", prompt=True, type=str, help="Path to local data file.")
def run(upload: bool = True, path_to_file: str = "") -> None:
    snap = paths.init_snapshot()
    snap.create_snapshot(filename=path_to_file, upload=upload)
