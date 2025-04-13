"""Script to create a snapshot of dataset."""

import datetime
import json
import time
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


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"bls/{SNAPSHOT_VERSION}/us_consumer_prices.csv")

    start_year = 1900
    end_year = datetime.date.today().year

    all_series_data = []
    headers = {"Content-type": "application/json"}
    for year in range(start_year, end_year + 1, 10):
        chunk_start = year
        chunk_end = min(year + 9, end_year)

        data = json.dumps({"seriesid": SERIES_IDS, "startyear": str(chunk_start), "endyear": str(chunk_end)})

        response = requests.post("https://api.bls.gov/publicAPI/v1/timeseries/data/", data=data, headers=headers)
        if response.status_code == 200:
            json_data = response.json()
            all_series_data.extend(json_data.get("Results", {}).get("series", []))
        else:
            print(f"Failed to get data for {chunk_start}-{chunk_end}: {response.status_code}")

            time.sleep(1)
    # Flatten into a list of dicts
    flattened = []
    for series in all_series_data:
        series_id = series["seriesID"]
        for item in series["data"]:
            flattened.append(
                {
                    "series_id": series_id,
                    "year": item["year"],
                    "period_name": item["periodName"],
                    "value": item["value"],
                }
            )

    # Convert to DataFrame and save
    df = pd.DataFrame(flattened)

    df_to_file(df, file_path=snap.path)

    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
