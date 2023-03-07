"""Script to create a snapshot of dataset 'Ozone hole area (NASA, 2023)'."""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
def main(upload: bool) -> None:
    files_dvc = [
        f"nasa/{SNAPSHOT_VERSION}/ozone_hole_area_p1.txt",
        f"nasa/{SNAPSHOT_VERSION}/ozone_hole_area_p2.txt",
    ]
    for f in files_dvc:
        # Create a new snapshot.
        snap = Snapshot(f)
        # Download data from source.
        snap.download_from_source()
        # Add file to DVC and upload to S3.
        snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
