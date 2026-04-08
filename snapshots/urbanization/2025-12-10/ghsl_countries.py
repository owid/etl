"""Script to create a snapshot of dataset."""

import tempfile
import zipfile
from pathlib import Path

import click
import requests

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"urbanization/{SNAPSHOT_VERSION}/ghsl_countries.xlsx")

    # Get the download URL from metadata
    assert snap.metadata.origin is not None, "Origin metadata is missing"
    download_url = snap.metadata.origin.url_download
    assert download_url is not None, "Download URL is not set in metadata"

    # Download the ZIP file and extract the XLSX file
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        zip_path = temp_path / "ghsl_data.zip"

        # Download ZIP file directly
        response = requests.get(download_url, stream=True)
        response.raise_for_status()

        with open(zip_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        # Extract XLSX file from ZIP
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            # Find the XLSX file in the ZIP
            xlsx_files = [f for f in zip_ref.namelist() if f.endswith(".xlsx")]
            if not xlsx_files:
                raise FileNotFoundError("No XLSX file found in the ZIP archive")

            # Extract the first XLSX file found
            xlsx_filename = xlsx_files[0]
            zip_ref.extract(xlsx_filename, temp_path)
            xlsx_path = temp_path / xlsx_filename

        # Copy extracted XLSX file to snapshots data folder, add file to DVC and upload to S3
        snap.create_snapshot(filename=str(xlsx_path), upload=upload)


if __name__ == "__main__":
    main()
