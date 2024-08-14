"""
Script to create a snapshot of dataset.

The data is uploaded manually from the Statistics Canada website.

STEPS TO CREATE A SNAPSHOT:
    1. Visit https://www150.statcan.gc.ca/t1/tbl1/en/cv!recreate.action?pid=1110013501&selectedNodeIds=2D1,3D1,4D2&checkedLevels=0D1&refPeriods=19760101,20220101&dimensionLayouts=layout2,layout2,layout2,layout2,layout3&vectorDisplay=false
    2. In "Customize table", make sure that Geography is set to "Canada". Unselect all other options (including subcategories).
    3. In "Persons in low income", only select "All persons". Unselect all other options (including subcategories).
    4. In "Low income lines", only select "Low income measure after tax".
    5. In "Statistics", only select "Percentage of persons in low income".
    6. Make sure that "Reference period" is set from the earliest year to the latest year.
    7. Click "Apply".
    8. Press "Download options" and select "CSV. Download selected data (for database loading)."
    9. Copy the downloaded file to this directory.
    10. Run this script with the path to the downloaded file as an argument.
        python snapshots/statistics_canada/2024-08-09/relative_poverty.py --path-to-file <path-to-file>

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
    snap = Snapshot(f"statistics_canada/{SNAPSHOT_VERSION}/relative_poverty.csv")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
