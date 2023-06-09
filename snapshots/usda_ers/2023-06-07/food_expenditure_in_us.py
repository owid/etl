"""Script to create a snapshot of dataset 'Food expenditure in United States'."""

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
    for file_name in ["food_expenditure_in_us_archive.xlsx", "food_expenditure_in_us.xlsx"]:
        # Create a new snapshot.
        snap = Snapshot(f"usda_ers/{SNAPSHOT_VERSION}/{file_name}")

        # Download data from source.
        snap.download_from_source()

        # Add file to DVC and upload to S3.
        snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
