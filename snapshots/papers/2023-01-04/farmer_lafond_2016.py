"""Ingest data from Farmer & Lafond (2016) paper.

The data was sent to Max Roser in 2016 in a private communication.
"""

from pathlib import Path

import click

from etl.snapshot import Snapshot

CURRENT_DIR = Path(__file__).parent
SNAPSHOT_VERSION = CURRENT_DIR.name


@click.command()
@click.option("--path-to-file", prompt=True, type=str, help="Path to local data file.")
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
def main(path_to_file: str, upload: bool) -> None:
    # Create new snapshot.
    snap = Snapshot(f"papers/{SNAPSHOT_VERSION}/farmer_lafond_2016.csv")
    # Ensure destination folder exists.
    snap.path.parent.mkdir(exist_ok=True, parents=True)
    # Copy local data file to snapshots data folder.
    snap.path.write_bytes(Path(path_to_file).read_bytes())
    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
