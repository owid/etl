"""
This code creates a snapshot of the relative poverty dataset from Statistics Finland.

The data needs to be uploaded manually following these steps:
    1. Open the StatFin platform https://pxdata.stat.fi/PxWeb/pxweb/en/StatFin/StatFin__eot/statfin_eot_pxt_13wk.px/
    2. In "Information", select "At-risk-of-poverty rate (threshold 60 % of median)"
    3. In "Year", click on "Select all".
    4. In "Age", click on "Total".
    5. In "Sex", click on "Total".
    6. In "Income concept:, select "Disposable cash income (excl. capital gains)"
    7. Click on "Show table".
    8. In the left panel, open "Save result as..." and select "Comma delimited without heading".
    9. Click on "Save".
    10. Copy the downloaded file to this folder.
    11. Run the script:
        python snapshots/statfin/{version}/relative_poverty.py --path-to-file {path_to_file}
    12. Delete the downloaded file.
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
    snap = Snapshot(f"statfin/{SNAPSHOT_VERSION}/gini_coefficient.csv")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
