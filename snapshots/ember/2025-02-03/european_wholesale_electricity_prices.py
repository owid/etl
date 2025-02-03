"""Script to create a snapshot of dataset.

NOTE: "date_published" is manually set in the dvc file as the latest date informed in the data.

"""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"ember/{SNAPSHOT_VERSION}/european_wholesale_electricity_prices.csv")

    # Download the data.
    snap.download_from_source()

    # Read the data to extract the latest date published.
    tb = snap.read()

    # Update metadata.
    snap.metadata.origin.date_published = tb["Date"].max()

    # Rewrite metadata to dvc file.
    snap.metadata_path.write_text(snap.metadata.to_yaml())

    # Finalize snapshot.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
