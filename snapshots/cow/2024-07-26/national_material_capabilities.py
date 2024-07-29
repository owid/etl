"""
Script to create a snapshot of dataset.

INSTRUCTIONS TO UPDATE THIS SNAPSHOT

    1. Visit https://correlatesofwar.org/data-sets/national-material-capabilities/
    2. Download the file "NMC_Documentation-{version}.zip". Download the latest version (it is currently version 6.0).
    3. Inside the zip file, extract the NMC-{version}-abridged.zip file.
    4. Inside this zip file, copy the file "NMC-{version}-abridged.csv" to the this folder.
    5. Run this script with the path to the file as an argument.
        python snapshots/cow/{version}/national_material_capabilities.py --path-to-file NMC-{version}-abridged.csv
"""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--path-to-file", prompt=True, type=str, help="Path to local data file.")
def main(path_to_file: str, upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"cow/{SNAPSHOT_VERSION}/national_material_capabilities.csv")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
