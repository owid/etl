"""Script to create a snapshot of the Federico–Tena World Population Historical Database (V2, 1991 borders)."""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"ggdc/{SNAPSHOT_VERSION}/federico_tena_population.tab")

    # Download data from source (url_download in the .dvc), add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    main()
