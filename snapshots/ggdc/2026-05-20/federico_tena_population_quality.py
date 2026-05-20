"""Script to create a snapshot of the Federico–Tena Quality Assessment file (2025, V2)."""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"ggdc/{SNAPSHOT_VERSION}/federico_tena_population_quality.xlsx")

    # Download data from source (url_download in the .dvc), add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    main()
