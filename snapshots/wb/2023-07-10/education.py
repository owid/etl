"""Script to create a snapshot of dataset 'World Bank Education Statistics (2023)'."""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from time import sleep

import click
import pandas as pd
import requests
import world_bank_data as wb
from owid.datautils.io import df_to_file
from tqdm import tqdm

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
@click.option("--path-to-file", prompt=True, type=str, help="Path to local data file.")
def main(path_to_file: str, upload: bool) -> None:
    """
    Downloads the csv file with indicator IDs for education and then accesses the data using the API to create
    a snapshot of the data.

    To create a csv with education related indicators and download it:
        - go to this wesbite http://databank.worldbank.org/Data/Views/VariableSelection/SelectVariables.aspx?source=Education%20Statistics
        - on the left click on Country -> select World, series -> select All and Time -> select All.
        - this is a workaround which lets you download all the Education related indicators and then access the data via an API.
        - downloading the entire dataset from the website won't work as there are restrictions on the data download size.
    """

    # Create a new snapshot.
    snap = Snapshot(f"wb/{SNAPSHOT_VERSION}/education.csv")

    # Ensure destination folder exists.
    snap.path.parent.mkdir(exist_ok=True, parents=True)

    wb_education_df = get_data(path_to_file)
    wb_education_df = add_metadata(wb_education_df)

    df_to_file(wb_education_df, file_path=snap.path)
    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


def fetch_education_data(education_code: str) -> pd.DataFrame:
    """
    Fetches education data for the given code.

    :param education_code: Education code for fetching data.
    :return: DataFrame with fetched data.
    """
    # Internet users indicator is different in the API so replace it with the right one
    if education_code == "IT.NET.USER.P2":
        education_code = "IT.NET.USER.ZS"

    # Access the data for indicator
    df = pd.DataFrame(wb.get_series(education_code)).reset_index()

    # Create a new column to keep the series code information (to access the metadata later)
    df["wb_seriescode"] = df.columns[-1]
    # Rename the column with values related to the indicator from series code to "value"
    df.rename(columns={df.columns[-2]: "value"}, inplace=True)
    df.dropna(subset=["value"], inplace=True)

    return df


def get_data(path_to_file: str) -> pd.DataFrame:
    """
    Reads the data with indicators from the given file path and fetches the education data for each indicator.

    :param path_to_file: Path to the CSV file.
    :return: DataFrame with education data.
    """
    # Read csv file with indicators
    indicators = pd.read_csv(
        path_to_file,
        encoding="latin-1",
        low_memory=False,
    )
    # Store unique indicator codes related to Education
    unique_series_codes = indicators["Series Code"].dropna().unique()

    with ThreadPoolExecutor() as executor:
        # Using list comprehension to run fetch_education_data concurrently
        wb_education = list(
            tqdm(
                executor.map(fetch_education_data, unique_series_codes),
                total=len(unique_series_codes),
                desc="Processing indicator",
            )
        )

    wb_education_df = pd.DataFrame()  # Create an empty DataFrame to store the result
    # The dataframes are really big which causes the kernel to break - the loop is a workaround.
    for df in tqdm(wb_education, desc="Concatenating dataframes"):
        wb_education_df = pd.concat([wb_education_df, df], ignore_index=True)
    return wb_education_df


def add_metadata(df):
    """
    Adds metadata (description and sources) by fetching details from the World Bank API using the world bank code.

    Args:
        df (DataFrame): The DataFrame containing wb_seriescode which is used for fetching metadata.

    Returns:
        DataFrame: The DataFrame with two new metadata columns - 'description' and 'source.

    Note:
        Each code needs to correspond to a World Bank indicator.
    """
    # Loop through the DataFrame columns
    for wb_code in tqdm(df["wb_seriescode"].unique(), desc="Processing metadata for indicators"):
        retries = 3
        delay = 1  # Initial delay in seconds

        # Attempt to process each column up to `retries` times
        for attempt in range(retries):
            try:
                # Construct the URL to fetch metadata from the World Bank API
                url = f"https://api.worldbank.org/v2/indicator/{wb_code}?format=json"

                # Send GET request to the World Bank API with a timeout (in seconds)
                response = requests.get(url, timeout=10)
                response.raise_for_status()  # Check for unsuccessful status codes

                data = response.json()

                # Validate the expected data format received from the World Bank API
                if len(data) < 2 or not isinstance(data[1], list) or len(data[1]) < 1:
                    raise ValueError("Unexpected data format received from the World Bank API")

                # Extract relevant metadata from the API response
                nested_data = data[1][0]

                # Update only the rows where 'wb_seriescode' matches the current 'wb_code'
                df.loc[df["wb_seriescode"] == wb_code, "description"] = nested_data["sourceNote"]
                df.loc[df["wb_seriescode"] == wb_code, "source"] = nested_data["sourceOrganization"]

            except requests.exceptions.ReadTimeout:
                print(f"Timeout while processing column {wb_code}. Retrying in {delay} seconds...")
                sleep(delay)
            delay *= 2  # Double the delay for the next attempt

    return df


if __name__ == "__main__":
    main()
