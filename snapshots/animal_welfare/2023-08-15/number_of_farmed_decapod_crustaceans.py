"""Script to create a snapshot of dataset."""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# File names of snapshots.
FILE_NAMES = [
    "number_of_farmed_decapod_crustaceans_2015.xlsx",
    "number_of_farmed_decapod_crustaceans_2016.xlsx",
    "number_of_farmed_decapod_crustaceans_2017.xlsx",
]


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
def main(upload: bool) -> None:
    for file_name in FILE_NAMES:
        # Create a new snapshot.
        snap = Snapshot(f"animal_welfare/{SNAPSHOT_VERSION}/{file_name}")

        # Download data from source.
        snap.download_from_source()

        # Add file to DVC and upload to S3.
        snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
