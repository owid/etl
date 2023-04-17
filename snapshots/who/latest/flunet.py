"""Script to create a snapshot of dataset 'FluNet, World Health Organization (2023)'."""

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
    snap = Snapshot(f"who/{SNAPSHOT_VERSION}/flunet.csv")

    # Download data from source.
    snap.download_from_source()

    # Try reading the csv, we sometimes get error invalid CSV with error
    # if this fails, don't upload the file
    pd.read_csv(snap.path)

    # Snapshot should have at least 20mb, otherwise something went wrong
    assert snap.path.stat().st_size > 20 * 2**20, "Snapshot file must have at least 20mb"

    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
