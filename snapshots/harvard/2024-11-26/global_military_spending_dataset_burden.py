"""
Script to create a snapshot of dataset.

The file is manually uploaded, because it is a small file from a 1.5 GB zip file.

STEPS TO UPDATE THIS SNAPSHOT

    1. Go to https://dataverse.harvard.edu/file.xhtml?fileId=8144788
    2. Download the file by selecting "Access File" and then "ZIP Archive".
    3. Unzip the file and copy the file in "data/milburden_all_xxxxx.rds" to this directory. xxxxx is the date of the latest version of the data.
    4. Run this script with the path to the file as an argument.
        python snapshots/harvard/{version}/global_military_spending_dataset_burden.py --path-to-file milburden_xxxxx.rds


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
    snap = Snapshot(f"harvard/{SNAPSHOT_VERSION}/global_military_spending_dataset_burden.rds")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
