"""Script to create a snapshot of dataset.

Scripts in this folder must be run in this order:
 1. snapshots/forests/2024-07-10/dominant_driver.py
 2. snapshots/forests/2024-07-10/reproject_raster.py
 3. Manual upload of the reprojected raster to Earth Engine assets
 4. python snapshots/forests/2024-07-10/run_earth_engine.py
 5. Grab the Google Sheet IDs from the output of the run_earth_engine.py script
 6. python tree_cover_loss_by_dominant_driver.py  - with the Google Sheet IDs
"""

from pathlib import Path

import click
import pandas as pd

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name
SHEET_IDS = [
    "13Toik5IlnJv45AwCuRXjsdybLXHRXEzUgVWP_OTh5h0",  # Countries 1-100
    "1cHoD5qHc5d-0cO2H2R43yL_x5Ikay1EKAQwhxsuNqYc",  # Countries 101-200
    "1_reAU6zxRknPLkwPxtwe2se0MYBVKTAElLre4V-tlfQ",  # Countries 201-250
    "1E2Wan-YJRSTlFHKGj2lpf-MNIOqvp7nFwsLG5EY-5oA",  # Countries 251-300
    "1TDqRy7xx2MwO7DwWK0sSh-rlvEzHiTVECm4RoT5DMOQ",  # Countries 301-400
]


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"forests/{SNAPSHOT_VERSION}/tree_cover_loss_by_driver.csv")

    tables = []
    for sheet_id in SHEET_IDS:
        # URL of the Google Sheet (make sure it is shared as 'Anyone with the link can view')
        sheet_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"
        # Read the CSV file
        print(f"Reading {sheet_url}")
        tb = pd.read_csv(sheet_url)
        assert tb.shape[0] >= 1
        tb = tb[["country", "year", "category", "area"]]
        tables.append(tb)

    tb = pd.concat(tables)

    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload, data=tb)


if __name__ == "__main__":
    main()
