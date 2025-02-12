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
BASE_URL = "https://crudata.uea.ac.uk/cru/data/soi/soi.dat"


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"climate/{SNAPSHOT_VERSION}/southern_oscillation_index.csv")

    response = requests.get(BASE_URL)
    data = response.text

    # Step 2: Process the data
    # Skip header lines and read into a DataFrame
    data_io = StringIO(data)
    df = pd.read_csv(data_io, sep="\s+", skiprows=6, header=None)

    # Assign column names
    columns = ["year"] + [f"month_{i}" for i in range(1, 13)] + ["annual"]
    df.columns = columns

    df = df.melt(id_vars=["year"], var_name="period_type", value_name="soi")
    df_to_file(df, file_path=snap.path)
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
