"""Script to create a snapshot of dataset.

The URL provided downloads a xlsx file that is erroneously saved as a Strict Open XML Spreadsheet (*.xlsx).
This is a bit of a difficult file to open in python, so to get around this we manually save the downloaded file
as a csv and then upload it via create_snapshot.

Download the file directly from here:

    https://ghdx.healthdata.org/record/ihme-data/gbd-2019-cause-rei-and-location-hierarchies
     - Click on Files (5) tab
     - Download 'Cause Hierarchy [XLSX]'

"""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--path-to-file", prompt=True, type=str, help="Path to local data file.")
def main(upload: bool, path_to_file: str) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"ihme_gbd/{SNAPSHOT_VERSION}/cause_hierarchy.csv")
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
