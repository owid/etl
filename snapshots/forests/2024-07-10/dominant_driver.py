"""Script to create a snapshot of dataset.

Scripts in this folder must be run in this order:
1. snapshots/forests/2024-07-10/dominant_driver.py
2. snapshots/forests/2024-07-10/reproject_raster.py
3. Manual upload of the reprojected raster to Earth Engine assets
4. snapshots/forests/2024-07-10/earth_engine.py

"""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"forests/{SNAPSHOT_VERSION}/dominant_driver.tif")

    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    main()
