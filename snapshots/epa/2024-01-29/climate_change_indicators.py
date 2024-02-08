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
    # Ocean heat content.
    "ocean_heat_content_annual_world_700m.csv",
    "ocean_heat_content_annual_world_2000m.csv",
    # Ice sheet mass balance.
    "ice_sheet_mass_balance.csv",
    # Greenhouse gas concentration.
    "co2_concentration.csv",
    "ch4_concentration.csv",
    "n2o_concentration.csv",
    # Cumulative mass balance of US glaciers.
    "mass_balance_us_glaciers.csv",
]


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot for each of the data files.
    for file_name in FILES:
        snap = Snapshot(f"epa/{SNAPSHOT_VERSION}/{file_name}")

        # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
        snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    main()
