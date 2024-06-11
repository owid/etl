"""
This script extracts data from the EqualDex API and creates two datasets:
- current.csv: current status of the issues in each country
- historical.csv: historical status of the issues in each country

The script also creates a long dataset that merges and expands the two previous datasets for each year.
This long dataset is saved as long.csv and it is indexed by country, year and issue.

To run this script, you need to add your API key to the .env file in this repository, as:
# Equaldex access key
EQUALDEX_KEY="your_api_key"
You can obtain your API key by registering at https://www.equaldex.com/ and then copying it from your account settings: https://www.equaldex.com/settings

After running this script, add the long.csv and the current.csv file to snapshots (change the date in the path for future updates):
    python snapshots/lgbt_rights/2024-06-03/equaldex.py --path-to-file snapshots/lgbt_rights/2024-06-03/long.csv
    python snapshots/lgbt_rights/2024-06-03/equaldex_current.py --path-to-file snapshots/lgbt_rights/2024-06-03/current.csv
    python snapshots/lgbt_rights/2024-06-03/equaldex_indices.py --path-to-file snapshots/lgbt_rights/2024-06-03/indices.csv

"""

import datetime
import json
import os
from pathlib import Path
from typing import List, Tuple

import pandas as pd
import requests
from structlog import get_logger
from tqdm import tqdm

# Set directory path
PARENT_DIR = Path(__file__).parent.absolute()

# Import API key (it is stored in .env file)
API_KEY = os.getenv("EQUALDEX_KEY")

# Set parameter to extract data from the API or not (this is useful to avoid running the API query every time)
GET_DATA_FROM_API = True

# Define regex pattern to find years in the data
REGEX_PATTERN = "(\d{4})"  # type: ignore

# Define current year
CURRENT_YEAR = datetime.datetime.now().year

# Define start year to fill current status as historical data
START_YEAR = 1950

# Define list of variables to extract for indices dataframe
VARIABLES_INDICES = ["name", "ei", "ei_legal", "ei_po"]

# Define country list
with open(PARENT_DIR / "country_list.json", "r") as f:
    COUNTRY_LIST = json.load(f)


# Function to extract data from the EqualDex API
def extract_from_api(country_list: List[str]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    # Initialize logger.
    log = get_logger()

    # Set equaldex parameters: url and headers
    url = "https://www.equaldex.com/api/region"
    headers = {"Content-Type": "application/json"}

    # Define empty dataframes. df_current is for current data and df_historical for historical data
    df_current = pd.DataFrame()
    df_historical = pd.DataFrame()
    df_indices = pd.DataFrame()

    # Create an empty list of the countries with no data
    countries_no_data = []

    if not API_KEY:
        log.fatal("API key not found. Please add your Equaldex API key to the .env file.")

    # For each country in the list
    for country in tqdm(country_list, desc="Extracting data from countries"):
        # Define query parameters
        querystring = {
            "regionid": country,
            # "formatted": "true",
            "apiKey": API_KEY,
        }

        # Run query, ensuring it delivers output with status 200
        # TODO: Make it less agressive. Pablo recommends "You could sleep in between requests, and add a maximum number of trials."
        status = 0
        while status != 200:
            response = requests.get(url, headers=headers, params=querystring, timeout=500)
            content = response.content
            status = response.status_code

            if status != 200:
                log.error(f"{country}: Status {status}")

        # Create a dictionary with the output
        response_dict = json.loads(content)  # type: ignore

        # Get the country name from the response
        try:
            country_name = response_dict["regions"]["region"]["name"]

        except Exception:
            continue

        # Get the list of issues available
        try:
            issue_list = list(response_dict["regions"]["region"]["issues"].keys())

        except Exception:
            # If no issue list is found, add this country to the "countries with no data" list
            countries_no_data.append(country)
            issue_list = []

        indices_data = pd.DataFrame()
        for variable in VARIABLES_INDICES:
            try:
                indices_data.loc[0, variable] = response_dict["regions"]["region"][variable]
            except Exception:
                indices_data.loc[0, variable] = None

        # Only concatenate if we have at least one column not null
        # Drop all null columns
        indices_data = indices_data.dropna(axis=1, how="all")
        if not indices_data.isnull().all().all():
            # Concatenate data from previous countries with current country
            df_indices = pd.concat([df_indices, indices_data], ignore_index=True)

        # For each issue on the list
        for issue in issue_list:
            # Get the current status
            try:
                current_data = pd.DataFrame(
                    response_dict["regions"]["region"]["issues"][issue]["current_status"],
                    index=[0],
                )

            except Exception:
                log.warning(f"{country_name}: No current data for {issue}")
                current_data = pd.DataFrame()

            # Get the history of the issue in the country
            try:
                historical_data = pd.DataFrame(
                    response_dict["regions"]["region"]["issues"][issue]["history"],
                )

            except Exception:
                historical_data = pd.DataFrame()

            # Add country name column to the dataframe
            current_data["country"] = country_name
            historical_data["country"] = country_name

            # Add issue column to the dataframe
            current_data["issue"] = issue
            historical_data["issue"] = issue

            # Concatenate data from previous countries with current country
            df_historical = pd.concat([df_historical, historical_data], ignore_index=True)
            df_current = pd.concat([df_current, current_data], ignore_index=True)

    # Info message with a summary of countries with no data
    if countries_no_data:
        log.info(f"Data was not found for the following {len(countries_no_data)} countries: \n{countries_no_data}")

    # Move country and issue to the beginning
    cols_to_move = ["country", "issue"]
    df_current = df_current[cols_to_move + [col for col in df_current.columns if col not in cols_to_move]]
    df_historical = df_historical[cols_to_move + [col for col in df_historical.columns if col not in cols_to_move]]

    # Add year_extraction column to the dataframe
    df_current["year_extraction"] = CURRENT_YEAR
    df_indices["year"] = CURRENT_YEAR

    # Export files
    df_current.to_csv(PARENT_DIR / "current.csv", index=False)
    df_historical.to_csv(PARENT_DIR / "historical.csv", index=False)
    df_indices.to_csv(PARENT_DIR / "indices.csv", index=False)

    return df_current, df_historical, df_indices


def create_long_dataset(df_current, df_historical):
    """
    Create a long dataset from current and historical data
    """

    # HISTORICAL DATA

    # Remove empty start_data_formatted and end_date_formatted
    df_historical = df_historical[
        ~(df_historical["start_date_formatted"].isnull()) & ~(df_historical["end_date_formatted"].isnull())
    ].reset_index(drop=True)

    # Get year after comma in the column name start_date_formatted and end_date_formatted
    df_historical["year_start"] = (
        df_historical["start_date_formatted"].str.extract(REGEX_PATTERN, expand=False).astype(int)
    )
    df_historical["year_end"] = df_historical["end_date_formatted"].str.extract(REGEX_PATTERN, expand=False).astype(int)

    # Create dataframe filling the years between year_start and year_end

    df_historical_long = pd.DataFrame()
    for i in range(len(df_historical)):
        df_country_issue = pd.DataFrame(
            {
                "country": df_historical.iloc[i]["country"],
                "year": range(
                    df_historical.iloc[i]["year_start"],
                    df_historical.iloc[i]["year_end"],
                ),
                "issue": df_historical.iloc[i]["issue"],
                "id": df_historical.iloc[i]["id"],
                "value": df_historical.iloc[i]["value"],
                "value_formatted": df_historical.iloc[i]["value_formatted"],
                "description": df_historical.iloc[i]["description"],
            }
        )
        df_historical_long = pd.concat([df_historical_long, df_country_issue], ignore_index=True)

    # Add historical identifier
    df_historical_long["dataset"] = "historical"

    # CURRENT DATA
    # The code for the current data is actually similar to the one for the historical data, but there are some differences, as end data is commonly null and the years are filled until current year
    # In the future, it would be good to create a function that can be used for both datasets

    # Fill start_date_formatted with START_YEAR if it is null
    df_current.loc[df_current["start_date_formatted"].isnull(), "date_modified"] = True
    df_current.loc[df_current["start_date_formatted"].isnull(), "start_date_formatted"] = f"Jan 1, {START_YEAR}"

    # Get year after comma in the column name start_date_formatted
    df_current["year_start"] = df_current["start_date_formatted"].str.extract(REGEX_PATTERN, expand=False).astype(int)

    # Obtain current year
    current_year = datetime.datetime.now().year

    # Create dataframe filling the years between year_start and year_end

    df_current_long = pd.DataFrame()
    for i in range(len(df_current)):
        df_country_issue = pd.DataFrame(
            {
                "country": df_current.iloc[i]["country"],
                "year": range(df_current.iloc[i]["year_start"], current_year + 1),
                "issue": df_current.iloc[i]["issue"],
                "id": df_current.iloc[i]["id"],
                "value": df_current.iloc[i]["value"],
                "value_formatted": df_current.iloc[i]["value_formatted"],
                "description": df_current.iloc[i]["description"],
                "date_modified": df_current.iloc[i]["date_modified"],
            }
        )
        df_current_long = pd.concat([df_current_long, df_country_issue], ignore_index=True)

    # Add current identifier
    df_current_long["dataset"] = "current"

    # Concatenate historical and current data
    df_long = pd.concat([df_current_long, df_historical_long], ignore_index=True)

    # Keep data only from START_YEAR
    df_long = df_long[df_long["year"] >= START_YEAR]

    # Fill missing date_modified with False
    df_long["date_modified"] = df_long["date_modified"].fillna(False)

    # Delete duplicates with the same id
    df_long = df_long.drop_duplicates(subset=["country", "year", "issue", "id"], keep="first")

    # Sort values by country, year, issue, dataset and date_modified
    df_long = df_long.sort_values(
        by=[
            "country",
            "year",
            "issue",
            "date_modified",
            "dataset",
        ],
        ascending=True,
    )

    # Show rows with duplicated index
    df_duplicated = df_long[df_long.duplicated(subset=["country", "year", "issue", "date_modified"], keep=False)].copy()  # type: ignore

    df_duplicated.to_csv(PARENT_DIR / "duplicated.csv", index=True)

    df_long.to_csv(PARENT_DIR / "long.csv", index=False)


# Define if data should be extracted from API or read from file
if GET_DATA_FROM_API:
    # Run API to extract data
    df_current, df_historical, df_indices = extract_from_api(COUNTRY_LIST)

else:
    # Read data from file
    df_current = pd.read_csv(PARENT_DIR / "current.csv")
    df_historical = pd.read_csv(PARENT_DIR / "historical.csv")

# Create long dataset, with country, year and issue as index. It also generates a file with duplicated rows
create_long_dataset(df_current, df_historical)
