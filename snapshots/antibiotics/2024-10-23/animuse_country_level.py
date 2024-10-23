"""Script to create a snapshot of dataset.

To get this data you must follow these steps:

- Go here: https://amu.woah.org/amu-system-portal/amu-data
- Click 'COUNTRY DATA'
- Navigate to the 'mg per kg' tab
- Click 'Select all' for the years
- Hover over the top right of the graph area to find the download button (...)
- Click this and select 'Export data'
- Select as a csv
- Click 'Export'

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
    snap = Snapshot(f"antibiotics/{SNAPSHOT_VERSION}/animuse_country_level.csv")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
