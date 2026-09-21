"""
Script to create a snapshot of dataset.

INSTRUCTIONS TO UPDATE THIS SNAPSHOT

    1. Download the versioned archive from the Correlates of War site — currently
       https://correlatesofwar.org/wp-content/uploads/NMCv7.zip (linked from
       https://correlatesofwar.org/data-sets/national-material-capabilities/).
       NOTE: the host serves an incomplete TLS certificate chain that the verified ETL
       downloader rejects, so a plain `url_download` snapshot fails. Download it with a
       browser or curl and pass the archive here — do NOT extract it.
    2. Run this script with the path to the downloaded zip:
        etls cow/<version>/national_material_capabilities.zip --path-to-file NMCv7.zip

    The archive is a zip-in-zip; the meadow step unpacks it and reads the abridged CSV.
"""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--path-to-file", prompt=True, type=str, help="Path to the downloaded NMC archive (zip).")
def main(path_to_file: str, upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"cow/{SNAPSHOT_VERSION}/national_material_capabilities.zip")

    # Copy the local archive to the snapshots data folder, add it to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)
