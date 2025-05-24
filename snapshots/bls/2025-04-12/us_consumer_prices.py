"""Script to create a snapshot of dataset."""

import datetime
import json
import os
from pathlib import Path

import click
import pandas as pd
import requests
from dotenv import load_dotenv
from owid.datautils.io import df_to_file
from structlog import get_logger

from etl.snapshot import Snapshot

# Initialize log.
log = get_logger()

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Get the API key from environment (needs to be in .env file). You get an email with the key when you register here https://data.bls.gov/registrationEngine/
# Load environment variables from .env file
load_dotenv()
KEY = os.getenv("US_BLS_API_KEY")
SERIES_IDS = [
    "CUUR0000SEEB01",  # College tuition fees
    "CUUR0000SAE1",  # Education aggregate
    "CUUR0000SEEB03",  # Day care and preschool
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

        data = json.dumps(
            {"seriesid": SERIES_IDS, "startyear": str(chunk_start), "endyear": str(chunk_end), "annualaverage": True}
        )
        params = {"registrationkey": KEY}

        response = requests.post(
            "https://api.bls.gov/publicAPI/v2/timeseries/data/", params=params, data=data, headers=headers
        )

        try:
            json_data = response.json()
        except json.JSONDecodeError:
            log.info(f"Failed to decode JSON for {chunk_start}-{chunk_end}: {response.text}")
            continue

        if response.status_code == 200 and json_data.get("status") == "REQUEST_SUCCEEDED":
            all_series_data.extend(json_data.get("Results", {}).get("series", []))
        else:
            log.info(f"API error: {json_data.get('message')[0]}")
            break  # Stop on any API-reported error

    if not all_series_data:
        raise ValueError("No data returned from the API.")
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
