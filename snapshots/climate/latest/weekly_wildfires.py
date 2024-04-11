"""Script to create a snapshot of dataset. Loads data from the EFFIS API and creates a snapshot of the dataset with weekly wildire numbers, area burnt and emissions.
This script generates data from 2003-2023. The data for the year 2024 and above will be processed separately to avoid long processing times."""

import datetime as dt
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
# Define a range of years for which data will be processed.

YEAR = 2024

TB_REGIONS = find(table="regions", dataset="regions").iloc[0].load().reset_index()
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


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Initialize a new snapshot object for storing data, using a predefined file path structure.
    snap = Snapshot(f"climate/{SNAPSHOT_VERSION}/weekly_wildfires.csv")

    # Add date_accessed
    snap = modify_metadata(snap)

    # Initialize an empty list to hold DataFrames for wildfire data.
    dfs_fires = []

    # Iterate through each country in the COUNTRIES dictionary.
    for country, country_name in tqdm(
        COUNTRIES.items(), desc=f"Processing number of fires and area burnt data for countries {YEAR}"
    ):
        # Format the API request URL for weekly wildfire data with current country and year.
        base_url = (
            "https://api2.effis.emergency.copernicus.eu/statistics/v2/gwis/weekly?country={country_code}&year={year}"
        )
        url = base_url.format(country_code=country, year=YEAR)
        # timeout after 30s, they have occasional outages
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            data = response.json()

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

    # Combine all individual fire DataFrames into one.
    dfs_fires = pd.concat(dfs_fires)

    # Similar process as above is repeated for emissions data, stored in `dfs_emissions`.
    dfs_emissions = []
    # Iterate through each country for emission data.
    for country, country_name in tqdm(
        COUNTRIES.items(), desc=f"Processing emissions data for countries for year {YEAR}"
    ):
        # Format the API request URL for weekly emissions data.
        base_url = "https://api2.effis.emergency.copernicus.eu/statistics/v2/emissions/weekly?country={country_code}&year={year}"

        url = base_url.format(country_code=country, year=YEAR)
        response = requests.get(url)
        if response.status_code == 200:
            # Parse the emissions data from the JSON response.
            data = response.json()

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
    dfs_emissions = pd.concat(dfs_emissions)

    # Combine both fires and emissions data into a final DataFrame.
    df_final = pd.concat([dfs_fires, dfs_emissions])
    # Save the final DataFrame to the specified file path in the snapshot.
    df_to_file(df_final, file_path=snap.path)

    # Add the file to DVC and optionally upload it to S3, based on the `upload` parameter.
    snap.dvc_add(upload=upload)


def modify_metadata(snap: Snapshot) -> Snapshot:
    snap.metadata.origin.date_published = dt.date.today()  # type: ignore
    snap.metadata.origin.date_accessed = dt.date.today()  # type: ignore
    snap.metadata.save()
    return snap


if __name__ == "__main__":
    main()
