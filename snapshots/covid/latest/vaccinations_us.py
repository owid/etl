"""Script to create a snapshot of dataset."""

from datetime import datetime, timedelta
from pathlib import Path
from urllib.error import HTTPError

import click
import pandas as pd

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name
# URL
BASE_URL = "https://github.com/owid/covid-19-data/raw/master/scripts/input/cdc/vaccinations"
DATE_MIN = "2020-12-20"
DATE_MAX = "2023-05-10"


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"covid/{SNAPSHOT_VERSION}/vaccinations_us.csv")

    # Read data
    dfs = []
    start_date = datetime.strptime(DATE_MIN, "%Y-%m-%d")
    end_date = datetime.strptime(DATE_MAX, "%Y-%m-%d")
    delta = end_date - start_date
    for i in range(delta.days + 1):
        # Build URL
        current_date = (start_date + timedelta(days=i)).strftime('%Y-%m-%d')
        url = f"{BASE_URL}/cdc_data_{current_date}.csv"

        # Read CSV
        print(f"{current_date} ({i}/{delta.days + 1})", end="\r")
        try:
            df = pd.read_csv(url, na_values=[0.0, 0])
        except HTTPError:
            print(f"Error: {current_date}")
            continue

        # Save frame
        dfs.append(df)

    # Concatenate
    df = pd.concat(dfs, ignore_index=True)

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(data=df, upload=upload)  # type: ignore


if __name__ == "__main__":
    main()
