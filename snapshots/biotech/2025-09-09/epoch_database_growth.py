"""Script to create a snapshot of dataset.

The underlying data comes from a gSheet Epoch AI has shared with me. I've linked it in the corresponding issue on github: https://github.com/owid/owid-issues/issues/2076


I've downloaded that sheet as an xlsx file and uploaded it as a snapshot using this script."""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--path-to-file", "-f", prompt=True, type=str, help="Path to local data file.")
def run(path_to_file: str, upload: bool) -> None:
    # Initialize a new snapshot.
    snap = Snapshot(f"biotech/{SNAPSHOT_VERSION}/epoch_database_growth.xlsx")

    # Save snapshot.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    run()
