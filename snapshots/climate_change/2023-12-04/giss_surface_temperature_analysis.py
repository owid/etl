"""Script to create a snapshot of dataset."""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Names of data files from GISS NASA.
FILES = [
    "giss_surface_temperature_analysis_world.csv",
    "giss_surface_temperature_analysis_northern_hemisphere.csv",
    "giss_surface_temperature_analysis_southern_hemisphere.csv",
]


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot for each of the data files.
    for file in FILES:
        snap = Snapshot(f"climate_change/{SNAPSHOT_VERSION}/{file}")

        # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
        snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    main()
