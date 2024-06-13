"""Script to create a snapshot of dataset."""

import datetime
import io
from pathlib import Path

import click
import pandas as pd
import requests
from owid.datautils.io import df_to_file

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

SERIES_IDS = [
    "CUUR0000SEEB01",  # College tuition fees
    "CUUR0000SAE1",  # Tuition, other school fees, and childcare
    "CUUR0000SEEB",  # Childcare
    "CUUR0000SAM",  # Medical care
    "CUUR0000SAH21",  # Household energy
    "CUUR0000SAH",  # Housing
    "CUUR0000SAF",  # Food & Beverages
    "CUUR0000SETG",  # Public transport
    "CUUS0000SS45011",  # New cars
    "CUUR0000SAA",  # Clothing
    "CUUR0000SEEE02",  # Computer software and accessories
    "CUUR0000SERE01",  # Toys
    "CUUR0000SERA01",  # TVs
    "CUUR0000SA0",  # All items
]


def fetch_one(series_id: str) -> pd.DataFrame:
    print(series_id)
    response = requests.post(
        "https://beta.bls.gov/dataViewer/csv",
        data={
            "selectedSeriesIds": series_id,
            "startYear": 1900,
            "endYear": datetime.date.today().year,
        },
    )
    return pd.read_csv(io.BytesIO(response.content))


def fetch_many(series_ids):
    return pd.concat([fetch_one(id) for id in series_ids])


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"bls/{SNAPSHOT_VERSION}/us_consumer_prices.csv")

    # Download data from source, add file to DVC and upload to S3.
    df = fetch_many(series_ids=SERIES_IDS)

    df_to_file(df, file_path=snap.path)

    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
