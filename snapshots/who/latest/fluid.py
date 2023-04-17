"""Script to create a snapshot of dataset 'FluID, World Health Oragnization (2023)'."""

from pathlib import Path

import click
import pandas as pd

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
    # Create a new snapshot.
    snap = Snapshot(f"who/{SNAPSHOT_VERSION}/fluid.csv")

    # Download data from source.
    snap.download_from_source()

    # Try reading the csv, we sometimes get error invalid CSV with error
    # ParserError: Error tokenizing data. C error: Expected 49 fields in line 50053, saw 56
    # if this fails, don't upload the file
    pd.read_csv(snap.path)

    # Snapshot should have at least 100mb, otherwise something went wrong
    assert snap.path.stat().st_size > 100 * 2**20, "Snapshot file must have at least 100mb"

    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
