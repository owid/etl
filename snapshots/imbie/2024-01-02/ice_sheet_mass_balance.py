"""Script to create snapshots of EPA compilations of different climate change indicators.

The main page is https://www.epa.gov/climate-indicators/view-indicators
"""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Names of data files.
FILES = [
    # Ocean Heat Content.
    "ice_sheet_mass_balance_antarctica.csv",
    "ice_sheet_mass_balance_greenland.csv",
]


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot for each of the data files.
    for file_name in FILES:
        snap = Snapshot(f"imbie/{SNAPSHOT_VERSION}/{file_name}")

        # Download file snapshot to data folder, add file to DVC and upload to S3.
        snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    main()
