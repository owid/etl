"""Script to create a snapshot of dataset 'World Bank Gender Statistics."""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import click
import pandas as pd
import world_bank_data as wb
from owid.repack import repack_frame
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
    Reads an excel file with metadata on Gender Statistics and then accesses the data based on the indicator IDs via the World Bank API.

    To create a metadata excel file with World Bank Gender Statistics related indicators and download it:
        - go to this wesbite https://databank.worldbank.org/source/gender-statistics#
        - on the left click on Country -> select All countries, series -> select All and Time -> select All.
        - download the metadata file (it will include codes for all the indicators (which we can use for the API) and related metadata)
        - this is a workaround which lets you download all the Gender Statistics related indicators and then access the data via an API.
        - there is also a way to download the entire dataset from the website https://genderdata.worldbank.org/ but it's possible that using the API will mean the updates to the dataset can be more frequent/up to date.
    """

    # Create a new snapshot.
    snap = Snapshot(f"wb/{SNAPSHOT_VERSION}/gender_statistics.feather")

    # Ensure destination folder exists.
    snap.path.parent.mkdir(exist_ok=True, parents=True)

    # Read the metadata file
    # Load metadata from the provided Excel file
    metadata_columns = [
        "Code",
        "License Type",
        "Indicator Name",
        "Long definition",
        "Source",
        "Topic",
        "Statistical concept and methodology",
        "Development relevance",
        "Limitations and exceptions",
        "General comments",
        "Notes from original source",
    ]
    metadata_df = pd.read_excel(path_to_file, usecols=metadata_columns, sheet_name="Series - Metadata")

    # Rename columns to lower case with underscores for consistency
    metadata_df.columns = [col.lower().replace(" ", "_") for col in metadata_df.columns]

    # Rename the 'code' column to 'wb_seriescode' to match the key in the input DataFrame
    metadata_df.rename(columns={"code": "wb_seriescode"}, inplace=True)

    wb_codes_to_extract = metadata_df["wb_seriescode"].unique().tolist()

    # Fetch data from the World Bank API.
    wb_df = get_data(wb_codes_to_extract)

    # Merge the input DataFrame with the metadata DataFrame
    enriched_df = pd.merge(wb_df, metadata_df, on="wb_seriescode", how="left")

    # Try to reduce the size of the dataframe
    enriched_df = repack_frame(enriched_df)

    # Write DataFrame to file.
    enriched_df.reset_index().to_feather(snap.path)

    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


def fetch_gender_data(wb_code: str) -> pd.DataFrame:
    """
    Fetches WB Gender Statistics data for the given code from the World Bank API.

    Args:
        wb_code (str): WB code for fetching data.

    Returns:
        DataFrame: DataFrame with fetched data or None if an error occurs.
    """
    try:
        # Fetch data for the given indicator code
        # This is a placeholder for the actual data fetching function
        data_series = wb.get_series(wb_code)

        # Convert the series to a DataFrame and reset the index
        df = data_series.to_frame("value").reset_index()
        df["wb_seriescode"] = wb_code
        df.dropna(subset=["value"], inplace=True)

        return df
    except ValueError as e:
        print(f"ValueError: {e}")
    except Exception as e:
        print(f"An error occurred while fetching the data: {e}")

    return pd.DataFrame()  # Return an empty DataFrame in case of an error


def get_data(wb_ids):
    """
    Reads the data for each indicator ID in wb_ids and fetches WB data for each indicator.

    Args:
        wb_ids (list): List of WB indicators.

    Returns:
        DataFrame: DataFrame with WB data for all indicators.
    """

    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(fetch_gender_data, code) for code in wb_ids]
        wb_gender = [f.result() for f in tqdm(futures, total=len(wb_ids), desc="Fetching data")]

    # Concatenate all non-empty dataframes efficiently
    wb_df = pd.concat(wb_gender, ignore_index=True)

    return wb_df


if __name__ == "__main__":
    main()
