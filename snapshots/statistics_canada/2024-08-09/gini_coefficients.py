"""
Script to create a snapshot of dataset.

The data is uploaded manually from the Statistics Canada website.

STEPS TO CREATE A SNAPSHOT:
    1. Visit https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1110013401
    2. In "Customize table", make sure that Geography is set to "Canada" and "Reference period" is set from the earliest year to the latest year.
    3. Click "Apply".
    4. Press "Download options" and select "CSV. Download selected data (for database loading)."
    5. Copy the downloaded file to this directory.
    6. Run this script with the path to the downloaded file as an argument.
        python snapshots/statistics_canada/2024-08-09/gini_coefficients.py --path-to-file <path-to-file>

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
    snap = Snapshot(f"statistics_canada/{SNAPSHOT_VERSION}/gini_coefficients.csv")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
