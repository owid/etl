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
    python snapshots/lgbt_rights/2023-04-13/equaldex.py --path-to-file snapshots/lgbt_rights/2023-04-13/long.csv
    python snapshots/lgbt_rights/2023-04-13/equaldex_current.py --path-to-file snapshots/lgbt_rights/2023-04-13/current.csv

"""

import datetime
import json
import os
from pathlib import Path
from typing import List, Tuple

import pandas as pd
import requests
from structlog import get_logger

# Set directory path
PARENT_DIR = Path(__file__).parent.absolute()

# Import API key (it is stored in .env file)
API_KEY = os.getenv("EQUALDEX_KEY")

# Set parameter to extract data from the API or not (this is useful to avoid running the API query every time)
GET_DATA_FROM_API = True


# Function to extract data from the EqualDex API
def extract_from_api(country_list: List[str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # Initialize logger.
    log = get_logger()

    # Set equaldex parameters: url and headers
    url = "https://www.equaldex.com/api/region"
    headers = {"Content-Type": "application/json"}

    # Define empty dataframes. df_current is for current data and df_historical for historical data
    df_current = pd.DataFrame()
    df_historical = pd.DataFrame()

    # Create an empty list of the countries with no data
    countries_no_data = []

    # For each country in the list
    for country in country_list:
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
            country_name = ""

        # Get the list of issues available
        try:
            issue_list = list(response_dict["regions"]["region"]["issues"].keys())

        except Exception:
            # If no issue list is found, add this country to the "countries with no data" list
            countries_no_data.append(country)
            issue_list = []

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
                log.warning(f"{country_name}: No historical data for {issue}")
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

    # Error message with a summary of countries with no data
    if countries_no_data:
        log.error(f"Data was not found for the following {len(countries_no_data)} countries: \n{countries_no_data}")

    # Move country and issue to the beginning
    cols_to_move = ["country", "issue"]
    df_current = df_current[cols_to_move + [col for col in df_current.columns if col not in cols_to_move]]
    df_historical = df_historical[cols_to_move + [col for col in df_historical.columns if col not in cols_to_move]]

    # Obtain current year
    current_year = datetime.datetime.now().year

    # Add year_extraction column to the dataframe
    df_current["year_extraction"] = current_year

    # Export files
    df_current.to_csv(PARENT_DIR / "current.csv", index=False)
    df_historical.to_csv(PARENT_DIR / "historical.csv", index=False)

    return df_current, df_historical


# Function to create a long dataset from current and historical data
def create_long_dataset(df_current, df_historical):
    # HISTORICAL DATA

    # Remove empty start_data_formatted and end_date_formatted
    df_historical = df_historical[
        ~(df_historical["start_date_formatted"].isnull()) & ~(df_historical["end_date_formatted"].isnull())
    ].reset_index(drop=True)

    # Get year after comma in the column name start_date_formatted and end_date_formatted
    df_historical["year_start"] = df_historical["start_date_formatted"].str.split(",").str[1].str.strip().astype(int)
    df_historical["year_end"] = df_historical["end_date_formatted"].str.split(",").str[1].str.strip().astype(int)

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

    # CURRENT DATA
    # The code for the current data is actually similar to the one for the historical data, but there are some differences, as end data is commonly null and the years are filled until current year
    # In the future, it would be good to create a function that can be used for both datasets

    # Remove empty start_data_formatted
    df_current = df_current[~df_current["start_date_formatted"].isnull()].reset_index(drop=True)

    # Get year after comma in the column name start_date_formatted
    df_current["year_start"] = df_current["start_date_formatted"].str.split(",").str[1].str.strip().astype(int)

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
            }
        )
        df_current_long = pd.concat([df_current_long, df_country_issue], ignore_index=True)

    # Concatenate historical and current data
    df_long = pd.concat([df_historical_long, df_current_long], ignore_index=True)

    df_long.to_csv(PARENT_DIR / "long.csv", index=False)

    # Set index as country, year and issue and verify that there are no duplicates
    df_long = df_long.set_index(["country", "year", "issue"], verify_integrity=False).sort_index()

    # Show rows with duplicated index
    df_duplicated = df_long[df_long.index.duplicated(keep=False)]  # type: ignore

    df_duplicated.to_csv(PARENT_DIR / "duplicated.csv", index=True)


# Define country list
country_list = [
    "AF",
    "AL",
    "DZ",
    "AS",
    "AD",
    "AO",
    "AI",
    "AQ",
    "AG",
    "AR",
    "AM",
    "AW",
    "AU",
    "AT",
    "AZ",
    "BS",
    "BH",
    "BD",
    "BB",
    "BY",
    "BE",
    "BZ",
    "BJ",
    "BM",
    "BT",
    "BO",
    "BQ",
    "BA",
    "BW",
    "BV",
    "BR",
    "IO",
    "BN",
    "BG",
    "BF",
    "BI",
    "CV",
    "KH",
    "CM",
    "CA",
    "KY",
    "CF",
    "TD",
    "CL",
    "CN",
    "CX",
    "CC",
    "CO",
    "KM",
    "CD",
    "CG",
    "CK",
    "CR",
    "HR",
    "CU",
    "CW",
    "CY",
    "CZ",
    "CI",
    "DK",
    "DJ",
    "DM",
    "DO",
    "EC",
    "EG",
    "SV",
    "GQ",
    "ER",
    "EE",
    "SZ",
    "ET",
    "FK",
    "FO",
    "FJ",
    "FI",
    "FR",
    "GF",
    "PF",
    "TF",
    "GA",
    "GM",
    "GE",
    "DE",
    "GH",
    "GI",
    "GR",
    "GL",
    "GD",
    "GP",
    "GU",
    "GT",
    "GG",
    "GN",
    "GW",
    "GY",
    "HT",
    "HM",
    "VA",
    "HN",
    "HK",
    "HU",
    "IS",
    "IN",
    "ID",
    "IR",
    "IQ",
    "IE",
    "IM",
    "IL",
    "IT",
    "JM",
    "JP",
    "JE",
    "JO",
    "KZ",
    "KE",
    "KI",
    "KP",
    "KR",
    "KW",
    "KG",
    "LA",
    "LV",
    "LB",
    "LS",
    "LR",
    "LY",
    "LI",
    "LT",
    "LU",
    "MO",
    "MG",
    "MW",
    "MY",
    "MV",
    "ML",
    "MT",
    "MH",
    "MQ",
    "MR",
    "MU",
    "YT",
    "MX",
    "FM",
    "MD",
    "MC",
    "MN",
    "ME",
    "MS",
    "MA",
    "MZ",
    "MM",
    "NA",
    "NR",
    "NP",
    "NL",
    "NC",
    "NZ",
    "NI",
    "NE",
    "NG",
    "NU",
    "NF",
    "MK",
    "MP",
    "NO",
    "OM",
    "PK",
    "PW",
    "PS",
    "PA",
    "PG",
    "PY",
    "PE",
    "PH",
    "PN",
    "PL",
    "PT",
    "PR",
    "QA",
    "RO",
    "RU",
    "RW",
    "RE",
    "BL",
    "SH",
    "KN",
    "LC",
    "MF",
    "PM",
    "VC",
    "WS",
    "SM",
    "ST",
    "SA",
    "SN",
    "RS",
    "SC",
    "SL",
    "SG",
    "SX",
    "SK",
    "SI",
    "SB",
    "SO",
    "ZA",
    "GS",
    "SS",
    "ES",
    "LK",
    "SD",
    "SR",
    "SJ",
    "SE",
    "CH",
    "SY",
    "TW",
    "TJ",
    "TZ",
    "TH",
    "TL",
    "TG",
    "TK",
    "TO",
    "TT",
    "TN",
    "TM",
    "TC",
    "TV",
    "TR",
    "UG",
    "UA",
    "AE",
    "GB",
    "UM",
    "US",
    "UY",
    "UZ",
    "VU",
    "VE",
    "VN",
    "VG",
    "VI",
    "WF",
    "EH",
    "YE",
    "ZM",
    "ZW",
    "AX",
]

# Define if data should be extracted from API or read from file
if GET_DATA_FROM_API:
    # Run API to extract data
    df_current, df_historical = extract_from_api(country_list)

else:
    # Read data from file
    df_current = pd.read_csv(PARENT_DIR / "current.csv")
    df_historical = pd.read_csv(PARENT_DIR / "historical.csv")

# Create long dataset, with country, year and issue as index. It also generates a file with duplicated rows
create_long_dataset(df_current, df_historical)
