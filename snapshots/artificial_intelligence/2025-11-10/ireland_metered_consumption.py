"""This script creates a snapshot for Ireland Metered Consumption dataset.
To download the file go to  https://www.cso.ie/en/statistics/energy/datacentresmeteredelectricityconsumption/ and click on PxStat tables.
Select all checkoboxes under Statistic, Quarter, and Electricity Consumption. Then click on Download and select CSV format.
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
    snap = Snapshot(f"artificial_intelligence/{SNAPSHOT_VERSION}/ireland_metered_consumption.csv")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
