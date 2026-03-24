"""Script to create a snapshot of the UNU-WIDER Government Revenue Dataset.

Steps to obtain the data:
- Go to https://www.wider.unu.edu/project/grd-government-revenue-dataset
- Click on `Access full and additional datasets here`
- Fill the form and submit
- Download the Full dataset (Stata format). The name of the file should be similar to UNUWIDERGRD_{year}.dta_.zip
- Unzip the file and upload the Stata file (UNUWIDERGRD_{year}.dta) by running:
    python *this code relative path* --path-to-file *path to Stata file*


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
    snap = Snapshot(f"unu_wider/{SNAPSHOT_VERSION}/government_revenue_dataset.dta")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
