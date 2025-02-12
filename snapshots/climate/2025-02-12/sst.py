"""Script to create a snapshot of dataset."""

from io import StringIO
from pathlib import Path

import click
import pandas as pd
import requests
from owid.datautils.io import df_to_file

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name
BASE_URL = "https://www.cpc.ncep.noaa.gov/data/indices/sstoi.indices"


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"climate/{SNAPSHOT_VERSION}/sst.csv")

    response = requests.get(BASE_URL)
    data = response.text

    # Step 2: Process the data
    # Skip header lines and read into a DataFrame
    data_io = StringIO(data)
    df = pd.read_csv(data_io, sep="\s+", skiprows=1, header=None)

    # Assign column names
    columns = [
        "year",
        "month",
        "nino1_2",
        "nino1_2_anomaly",
        "nino3",
        "nino3_anomaly",
        "nino4",
        "nino4_anomaly",
        "nino3_4",
        "nino3_4_anomaly",
    ]
    df.columns = columns

    df_to_file(df, file_path=snap.path)
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
