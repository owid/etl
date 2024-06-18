"""
Script to create a snapshot of dataset.
The data is uploaded manually only because the website blocks the download request.
To update:
    1. Go to https://www.imf.org/external/datamapper/datasets/FPP and click the "Full data in Excel" link.
    2. Save the file in tis folder.
    3. Run the script.
        python snapshots/imf/{version}/public_finances_modern_history.py --path-to-file "snapshots/imf/{version}/{file_name}.xlsx"

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
    snap = Snapshot(f"imf/{SNAPSHOT_VERSION}/public_finances_modern_history.xlsx")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
