"""Ingest OPHI raw data: Multidimensional Poverty Index.
The file is not yet available in the website, so for now the data is uploaded manually
The file is currently obtained from an email by the OPHI team, but they said they would upload this very same file
"""

import pathlib
from pathlib import Path

import click

from etl.snapshot import Snapshot

SNAPSHOT_VERSION = "2022-12-13"

CURRENT_DIR = pathlib.Path(__file__).parent


@click.command()
@click.option("--path-to-file", prompt=True, type=str, help="Path to local data file.")
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload snapshot")
def main(path_to_file: str, upload: bool) -> None:
    # Create new snapshot.
    snap = Snapshot(f"ophi/{SNAPSHOT_VERSION}/multidimensional_poverty_index.csv")
    # Ensure destination folder exists.
    snap.path.parent.mkdir(exist_ok=True, parents=True)
    # Copy local data file to snapshots data folder.
    snap.path.write_bytes(Path(path_to_file).read_bytes())
    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
