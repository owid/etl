"""Script to create a snapshot of dataset."""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Year of current snapshot version.
CURRENT_YEAR = SNAPSHOT_VERSION.split("-")[0]

# Names of data files from GISS NASA.
FILES = [
    "surface_temperature_analysis_world.csv",
    "surface_temperature_analysis_northern_hemisphere.csv",
    "surface_temperature_analysis_southern_hemisphere.csv",
    "sea_ice_index.xlsx",
]

# To ease the recurrent task of updating these files, fetch the access date from the version, and write it to files.
# For simplicity, also assume that the publication date is the same as the access date.
DATE_ACCESSED = SNAPSHOT_VERSION
DATE_PUBLISHED = SNAPSHOT_VERSION


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot for each of the data files.
    for file_name in FILES:
        snap = Snapshot(f"climate_change/{SNAPSHOT_VERSION}/{file_name}")

        # Replace the full citation and description in the metadata.
        snap.metadata.origin.date_accessed = DATE_ACCESSED  # type: ignore
        snap.metadata.origin.date_published = DATE_PUBLISHED  # type: ignore

        if file_name == "sea_ice_index.xlsx":
            snap.metadata.origin.attribution = f"National Snow and Ice Data Center - Sea Ice Index ({CURRENT_YEAR})"  # type: ignore

        # Rewrite metadata to dvc file.
        snap.metadata_path.write_text(snap.metadata.to_yaml())

        # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
        snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    main()
