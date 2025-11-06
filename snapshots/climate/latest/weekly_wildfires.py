"""
Script to create a snapshot of dataset.
Loads data from the EFFIS API and creates a snapshot of the dataset with weekly wildfire numbers, area burnt and emissions.
This script generates data for the years 2024 and above. Historical data (2003–2023) is processed separately.
"""

import datetime as dt
import json
import subprocess
import time
from datetime import datetime
from pathlib import Path

import click
import pandas as pd
import requests
from owid.catalog import find
from owid.datautils.io import df_to_file
from structlog import get_logger
from tqdm import tqdm

from etl.snapshot import Snapshot

# Initialize logger.
log = get_logger()


# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Get the current year
CURRENT_YEAR = datetime.now().year

# Define a range of years including a starting year and all years up to the current year
START_YEAR = 2024  # Define the starting year
YEARS = list(range(START_YEAR, CURRENT_YEAR + 1))


TB_REGIONS = find(table="regions", dataset="regions").iloc[0].load().reset_index()
TB_REGIONS = TB_REGIONS[TB_REGIONS.defined_by == "owid"]
COUNTRIES = {code: name for code, name in zip(TB_REGIONS["code"], TB_REGIONS["name"])}

GWIS_SPECIFIC = {
    "XCA": "Caspian Sea",
    "XKO": "Kosovo under UNSCR 1244",
    "XAD": "Akrotiri and Dhekelia",
    "XNC": "Northern Cyprus",
}

COUNTRIES.update(GWIS_SPECIFIC)

EXCLUDE_OWID = ["OWID"]
EXCLUDE_ADDITIONAL = ["ANT", "ATA", "PS_GZA"]

COUNTRIES = {
    k: v for k, v in COUNTRIES.items() if not k.startswith(tuple(EXCLUDE_OWID)) and k not in EXCLUDE_ADDITIONAL
}

TIMEOUT = 300  # seconds


def fetch_with_curl(url: str) -> dict:
    """Fallback method using curl with retry logic."""
    try:
        result = subprocess.run(
            [
                "curl",
                "--fail",
                "--retry",
                "5",
                "--retry-all-errors",
                "--retry-delay",
                "2",
                "--silent",
                "--show-error",
                url,
            ],
            capture_output=True,
            text=True,
            timeout=TIMEOUT,
        )

        if result.returncode != 0:
            raise Exception(f"curl failed with code {result.returncode}: {result.stderr}")

        return json.loads(result.stdout)
    except subprocess.TimeoutExpired:
        raise Exception(f"curl request timed out after {TIMEOUT} seconds")
    except json.JSONDecodeError as e:
        raise Exception(f"Failed to parse JSON from curl response: {e}")


def fetch_with_retry(url: str, max_retries: int = 3, timeout: int = TIMEOUT) -> dict:
    """Fetch data from API with retry logic and comprehensive error handling."""
    headers = {"User-Agent": "Mozilla/5.0"}

    # First try with requests
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            data = response.json()
            return data
        except requests.exceptions.Timeout:
            log.warning(f"Timeout for {url}, attempt {attempt + 1}/{max_retries}")
        except requests.exceptions.ConnectionError:
            log.warning(f"Connection error for {url}, attempt {attempt + 1}/{max_retries}")
        except requests.exceptions.HTTPError as e:
            if response.status_code in [500, 502, 503, 504]:
                log.warning(f"Server error {response.status_code} for {url}, attempt {attempt + 1}/{max_retries}")
            elif response.status_code in [403, 429]:
                log.warning(
                    f"Rate limit/forbidden {response.status_code} for {url}, attempt {attempt + 1}/{max_retries}"
                )
            elif response.status_code == 404:
                log.info(f"No data available (404) for {url}")
                return {}  # Return empty dict to signal no data available
            else:
                log.error(f"HTTP error {response.status_code} for {url}: {e}")
                raise
        except requests.exceptions.JSONDecodeError:
            log.warning(f"JSON decode error for {url}, attempt {attempt + 1}/{max_retries}")
        except Exception as e:
            log.error(f"Unexpected error for {url}: {e}")
            raise

        if attempt < max_retries - 1:
            delay = max(2 ** (attempt + 1), 4)  # Start at 4s, then 8s, 16s...
            delay = min(delay, 12)  # Cap at 12 seconds
            log.info(f"Waiting {delay}s before retry...")
            time.sleep(delay)

    # If requests failed, try with curl as fallback
    log.info(f"Requests failed for {url}, trying curl fallback...")
    try:
        return fetch_with_curl(url)
    except Exception as e:
        log.error(f"Both requests and curl failed for {url}: {e}")
        # Exit script if we can't fetch data after all retries and fallback
        exit(1)


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Initialize a new snapshot object for storing data, using a predefined file path structure.
    snap = Snapshot(f"climate/{SNAPSHOT_VERSION}/weekly_wildfires.csv")

    # Load existing snapshot for comparison at the end of the script.
    try:
        orig_snapshot_df = snap.read()
    except FileNotFoundError:
        orig_snapshot_df = None

    # Initialize an empty list to hold DataFrames for wildfire data.
    dfs_fires = []
    for YEAR in YEARS:
        # Iterate through each country in the COUNTRIES dictionary.
        for country, _ in tqdm(
            COUNTRIES.items(), desc=f"Processing number of fires and area burnt data for countries {YEAR}"
        ):
            # Format the API request URL for weekly wildfire data with current country and year.
            base_url = "https://api2.effis.emergency.copernicus.eu/statistics/v2/gwis/weekly?country={country_code}&year={year}"
            url = base_url.format(country_code=country, year=YEAR)

            data = fetch_with_retry(url)
            if not data:  # Empty dict returned for 404s
                log.info(f"No fires data available for {country} {YEAR}")
                continue

            # Extract the weekly wildfire data.
            banfweekly = data["banfweekly"]
            # Convert the weekly data into a pandas DataFrame.
            df = pd.DataFrame(banfweekly)
            # Select and rename relevant columns, and calculate the 'month_day' column.
            df = df[["mddate", "events", "area_ha"]]
            df["month_day"] = [date[4:6] + "-" + date[6:] for date in df["mddate"]]

            # Add 'year' and 'country' columns with the current iteration values.
            df["year"] = YEAR
            df["country"] = COUNTRIES[country]

            # Reshape the DataFrame to have a consistent format for analysis, separating 'events' and 'area_ha' into separate rows.
            df_melted = pd.melt(
                df,
                id_vars=["country", "year", "month_day"],
                value_vars=["events", "area_ha"],
                var_name="indicator",
                value_name="value",
            )

            # Extract the cumulative wildfire data.
            banfcumulative = data["banfcumulative"]
            # Convert the cumulative data into a pandas DataFrame.
            df_cum = pd.DataFrame(banfcumulative)
            # Similar processing as above for the cumulative data.
            df_cum = df_cum[["mddate", "events", "area_ha"]]
            df_cum["month_day"] = [date[4:6] + "-" + date[6:] for date in df_cum["mddate"]]
            df_cum["year"] = YEAR
            df_cum["country"] = COUNTRIES[country]

            # Reshape the cumulative DataFrame to match the format of the weekly data, marking indicators as cumulative.
            df_melted_cum = pd.melt(
                df_cum,
                id_vars=["country", "year", "month_day"],
                value_vars=["events", "area_ha"],
                var_name="indicator",
                value_name="value",
            )
            df_melted_cum["indicator"] = df_melted_cum["indicator"].apply(lambda x: x + "_cumulative")

            # Concatenate the weekly and cumulative data into a single DataFrame.
            df_all = pd.concat([df_melted, df_melted_cum])

            # Append the processed DataFrame to the list of fire DataFrames.
            dfs_fires.append(df_all)

            # Short delay between requests to avoid rate limiting
            time.sleep(0.5)

    # Combine all individual fire DataFrames into one.
    if not dfs_fires:
        raise ValueError("No fires data was successfully fetched from any country/year combination")
    dfs_fires = pd.concat(dfs_fires)
    log.info(f"Successfully fetched fires data for {len(dfs_fires)} records")

    # Similar process as above is repeated for emissions data, stored in `dfs_emissions`.
    dfs_emissions = []
    for YEAR in YEARS:
        # Iterate through each country for emission data.
        for country, _ in tqdm(COUNTRIES.items(), desc=f"Processing emissions data for countries for year {YEAR}"):
            # Format the API request URL for weekly emissions data.
            base_url = "https://api2.effis.emergency.copernicus.eu/statistics/v2/emissions/weekly?country={country_code}&year={year}"
            url = base_url.format(country_code=country, year=YEAR)

            data = fetch_with_retry(url)
            if not data:  # Empty dict returned for 404s
                log.info(f"No emissions data available for {country} {YEAR}")
                continue

            # Extract and process the weekly emissions data.
            emiss_weekly = data["emissionsweekly"]
            df = pd.DataFrame(emiss_weekly)
            df["month_day"] = [date[4:6] + "-" + date[6:] for date in df["dt"]]

            # Select relevant columns and rename for consistency.
            df = df[["plt", "month_day", "curv"]]
            df["year"] = YEAR
            df["country"] = COUNTRIES[country]
            df = df.rename(columns={"plt": "indicator", "curv": "value"})

            # Process cumulative emissions data similarly.
            emiss_cumulative = data["emissionsweeklycum"]
            df_cum = pd.DataFrame(emiss_cumulative)
            df_cum["month_day"] = [date[4:6] + "-" + date[6:] for date in df_cum["dt"]]
            df_cum = df_cum[["plt", "month_day", "curv"]]
            df_cum["year"] = YEAR
            df_cum["country"] = COUNTRIES[country]
            df_cum = df_cum.rename(columns={"plt": "indicator", "curv": "value"})
            df_cum["indicator"] = df_cum["indicator"].apply(lambda x: x + "_cumulative")

            # Concatenate weekly and cumulative emissions data.
            df_all = pd.concat([df, df_cum])
            # Append to the list of emissions DataFrames.
            dfs_emissions.append(df_all)

    # Combine all emissions DataFrames into one.
    if not dfs_emissions:
        raise ValueError("No emissions data was successfully fetched from any country/year combination")
    dfs_emissions = pd.concat(dfs_emissions)
    log.info(f"Successfully fetched emissions data for {len(dfs_emissions)} records")

    # Combine both fires and emissions data into a final DataFrame.
    df_final = pd.concat([dfs_fires, dfs_emissions])

    if orig_snapshot_df is not None and len(df_final) < len(orig_snapshot_df):
        raise ValueError(
            f"New snapshot has fewer rows ({len(df_final)}) than the original snapshot {len(orig_snapshot_df)}. API could be down or data is missing."
        )

    # Save the final DataFrame to the specified file path in the snapshot.
    df_to_file(df_final, file_path=snap.path)  # type: ignore[reportArgumentType]

    # Add date_accessed
    snap = modify_metadata(snap)

    # Add the file to DVC and optionally upload it to S3, based on the `upload` parameter.
    snap.dvc_add(upload=upload)


def modify_metadata(snap: Snapshot) -> Snapshot:
    snap.metadata.origin.date_published = dt.date.today()  # type: ignore
    snap.metadata.origin.date_accessed = dt.date.today()  # type: ignore
    snap.metadata.save()
    return snap


if __name__ == "__main__":
    main()
