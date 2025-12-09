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

    # Fetch annual averages for all years up to end_year - 1
    for year in range(start_year, end_year, 10):
        chunk_start = year
        chunk_end = min(year + 9, end_year - 1)

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

    # Fetch monthly data for current year to calculate annual average
    log.info(f"Fetching monthly data for {end_year}")
    data = json.dumps({"seriesid": SERIES_IDS, "startyear": str(end_year), "endyear": str(end_year)})
    params = {"registrationkey": KEY}

    response = requests.post(
        "https://api.bls.gov/publicAPI/v2/timeseries/data/", params=params, data=data, headers=headers
    )

    try:
        json_data = response.json()
        if response.status_code == 200 and json_data.get("status") == "REQUEST_SUCCEEDED":
            all_series_data.extend(json_data.get("Results", {}).get("series", []))
    except json.JSONDecodeError:
        log.info(f"Failed to decode JSON for {end_year}: {response.text}")

    if not all_series_data:
        raise ValueError("No data returned from the API.")

    # Process data and calculate annual average for current year only
    flattened = []
    for series in all_series_data:
        series_id = series["seriesID"]

        # Group data by year to check for annual averages
        data_by_year = {}
        for item in series["data"]:
            year = item["year"]
            if year not in data_by_year:
                data_by_year[year] = {"monthly": [], "semi_annual": [], "annual": None}

            period = item["period"]
            if period in ("M13", "S03"):
                # Annual average exists (M13 for monthly series, S03 for semi-annual series)
                data_by_year[year]["annual"] = item
            elif period.startswith("M"):
                # Monthly data
                data_by_year[year]["monthly"].append(item)
            elif period.startswith("S"):
                # Semi-annual data (S01 = 1st half, S02 = 2nd half)
                data_by_year[year]["semi_annual"].append(item)

        # Add data to flattened list, calculating annual average only for current year if missing
        for year, year_data in data_by_year.items():
            if year_data["annual"]:
                # Use existing annual average
                flattened.append(
                    {
                        "series_id": series_id,
                        "year": year_data["annual"]["year"],
                        "period_name": year_data["annual"]["periodName"],
                        "value": year_data["annual"]["value"],
                    }
                )
            elif year == str(end_year):
                # Calculate annual average from available data for current year only
                if year_data["monthly"]:
                    # Calculate from monthly data
                    monthly_values = [float(item["value"]) for item in year_data["monthly"]]
                    avg_value = sum(monthly_values) / len(monthly_values)
                    flattened.append(
                        {
                            "series_id": series_id,
                            "year": year,
                            "period_name": "Annual",
                            "value": str(round(avg_value, 3)),
                        }
                    )
                    log.info(f"Calculated annual average for {series_id} in {year}: {avg_value:.3f}")
                elif year_data["semi_annual"]:
                    # Calculate from semi-annual data
                    semi_annual_values = [float(item["value"]) for item in year_data["semi_annual"]]
                    avg_value = sum(semi_annual_values) / len(semi_annual_values)
                    flattened.append(
                        {
                            "series_id": series_id,
                            "year": year,
                            "period_name": "Annual",
                            "value": str(round(avg_value, 3)),
                        }
                    )
                    log.info(
                        f"Calculated annual average for {series_id} in {year} from semi-annual data: {avg_value:.3f}"
                    )

    # Convert to DataFrame and save
    df = pd.DataFrame(flattened)

    df_to_file(df, file_path=snap.path)

    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
