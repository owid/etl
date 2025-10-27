"""Script to create a snapshot of dataset."""

import tempfile
import zipfile
from pathlib import Path
from urllib.request import urlretrieve

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"artificial_intelligence/{SNAPSHOT_VERSION}/epoch_gpus.csv")

    # Download the zip file and extract the CSV
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        zip_path = tmp_path / "ml_hardware.zip"

        # Download the zip file
        click.echo(f"Downloading {snap.metadata.origin.url_download}...")
        urlretrieve(snap.metadata.origin.url_download, zip_path)

        # Extract ml_hardware.csv from the zip
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extract("ml_hardware.csv", tmp_path)

        csv_path = tmp_path / "ml_hardware.csv"

        # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
        snap.create_snapshot(filename=str(csv_path), upload=upload)


if __name__ == "__main__":
    main()
