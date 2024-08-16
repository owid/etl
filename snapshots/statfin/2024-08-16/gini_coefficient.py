"""
This code creates a snapshot of the Gini coefficient dataset from Statistics Finland.

The data needs to be uploaded manually following these steps:
    1. Open the StatFin platform https://pxdata.stat.fi/PxWeb/pxweb/en/StatFin/StatFin__tjt/statfin_tjt_pxt_11x3.px
    2. In "Information", select "Gini coefficient(%)"
    3. In "Income concept", select "Disposable cash income (excl. capital gains, cross-nationally comparable concept, sample-based data)"
    4. In "Year", click on "Select all".
    5. Click on "Show table".
    6. Click on "Pivot clockwise".
    7. In the left panel, open "Save result as..." and select "Comma delimited without heading".
    8. Click on "Save".
    9. Copy the downloaded file to this folder.
    10. Run the script:
        python snapshots/statfin/{version}/gini_coefficient.py --path-to-file {path_to_file}
    11. Delete the downloaded file.
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
