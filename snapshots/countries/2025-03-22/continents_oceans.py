"""Script to create a snapshot of dataset."""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--path-to-file", "-f", prompt=True, type=str, help="Path to local data file.")
def run(path_to_file: str, upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"countries/{SNAPSHOT_VERSION}/continents_oceans.zip")

    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    run()
