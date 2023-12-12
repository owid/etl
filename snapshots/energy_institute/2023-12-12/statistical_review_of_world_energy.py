"""Script to create a snapshot of dataset 'Energy Institute Statistical Review of World Energy (2023)'."""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name
# Data files to snapshot.
SNAPSHOT_FILES = [
    "statistical_review_of_world_energy.csv",
    "statistical_review_of_world_energy.xlsx",
]


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
def main(upload: bool) -> None:
    # Create a new snapshot.
    for snapshot_file in SNAPSHOT_FILES:
        snap = Snapshot(f"energy_institute/{SNAPSHOT_VERSION}/{snapshot_file}")

        # Download data from source.
        snap.download_from_source()

        # Add file to DVC and upload to S3.
        snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
