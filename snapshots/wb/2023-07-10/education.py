"""Script to create a snapshot of dataset 'World Bank Education Statistics (2023)'."""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import click
import pandas as pd
import world_bank_data as wb
from owid.datautils.io import df_to_file
from tqdm import tqdm

from etl.db import get_engine
from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Series code that are either no longer available or had no data in November 2023
INVALID_EDUCATION_CODES = [
    "SE.PRM.TINM.1",
    "SE.PRM.TINM.2",
    "SE.PRM.TINM.3",
    "SE.PRM.TINM.4",
    "SE.PRM.TINM.5",
    "SE.PRM.TINM.6",
    "SE.PRM.TINM.7",
    "SE.PRM.TINM.8",
    "SE.PRM.TINM.9",
    "SE.PRM.TINM.10",
    "SE.LPV.PRIM.SD",
]


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
        - download the metadata file (it will include codes for all the indicators (which we can use for the API) and related metadata)
        - this is a workaround which lets you download all the Education related indicators and then access the data via an API.
        - downloading the entire dataset from the website won't work as there are restrictions on the data download size.
    """

    # Create a new snapshot.
    snap = Snapshot(f"wb/{SNAPSHOT_VERSION}/education.csv")

    # Ensure destination folder exists.
    snap.path.parent.mkdir(exist_ok=True, parents=True)

    # Fetch data from the World Bank API.
    wb_education_df = get_data()

    # Add metadata columns to the DataFrame.
    wb_education_df = add_metadata(wb_education_df, path_to_file)

    # Write DataFrame to file.
    df_to_file(wb_education_df, file_path=snap.path)

    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


def fetch_education_data(education_code: str) -> pd.DataFrame:
    """
    Fetches education data for the given code from the World Bank API.

    Args:
        education_code (str): Education code for fetching data.

    Returns:
        DataFrame: DataFrame with fetched data or None if an error occurs.
    """
    try:
        # Replace the indicator code if necessary
        education_code = "IT.NET.USER.ZS" if education_code == "IT.NET.USER.P2" else education_code
        # Fetch data for the given indicator code
        # This is a placeholder for the actual data fetching function
        data_series = wb.get_series(education_code)

        # Convert the series to a DataFrame and reset the index
        df = data_series.to_frame("value").reset_index()
        df["wb_seriescode"] = education_code
        df.dropna(subset=["value"], inplace=True)

        return df
    except ValueError as e:
        print(f"ValueError: {e}")
    except Exception as e:
        print(f"An error occurred while fetching the data: {e}")

    return pd.DataFrame()  # Return an empty DataFrame in case of an error


def get_data():
    """
    Reads the data with indicators from the given file path and fetches education data for each indicator.

    Args:
        path_to_file (str): Path to the file with metadata.

    Returns:
        DataFrame: DataFrame with education data for all indicators.
    """
    # Get the list of World Bank series codes from live Grapher
    wb_ids = used_world_bank_ids()

    # Some variables were created posthoc and don't use the standard World bank id convention
    wb_ids = [element for element in wb_ids if element is not None]

    # Add Wittgenstein Projection: Percentage of the population 25+ by highest level of educational attainment. No Education. Total
    wb_ids = wb_ids + ["PRJ.ATT.25UP.NED.MF"]

    # Assert that the list is not empty
    assert len(wb_ids) > 0, "The list wb_ids is empty after removing None elements."

    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(fetch_education_data, code) for code in wb_ids]
        wb_education = [f.result() for f in tqdm(futures, total=len(wb_ids), desc="Fetching data")]

    # Concatenate all non-empty dataframes efficiently
    wb_education_df = pd.concat(wb_education, ignore_index=True)

    return wb_education_df


def used_world_bank_ids():
    # This will connect to MySQL from specified ENV, so to run it against production you'd run
    # ETL=.env.prod python snapshots/wb/2023-07-10/education.py
    engine = get_engine()
    q = """
    select distinct
        SUBSTRING_INDEX(SUBSTRING(v.description, LOCATE('World Bank variable id: ', v.description) + LENGTH('World Bank variable id: ')), ' ', 1) AS wb_id,
        v.*
    from chart_dimensions as cd
    join charts as c on c.id = cd.chartId
    join variables as v on v.id = cd.variableId
    where v.datasetId = 6194
    """
    df = pd.read_sql(q, engine)
    return list(df["wb_id"].unique())


def add_metadata(df, path_to_file):
    """
    Enriches the input DataFrame with metadata columns based on the World Bank series code.

    The function reads an Excel file containing metadata and merges the input DataFrame with
    the metadata DataFrame based on the World Bank series code.

    Args:
        df (DataFrame): A pandas DataFrame containing the column 'wb_seriescode' which holds unique World Bank series codes.
        path_to_file (str): The file path to an Excel file containing metadata associated with World Bank series codes.

    Returns:
        DataFrame: The original DataFrame enriched with additional metadata columns.

    Notes:
        The Excel file should contain the following columns: 'Code', 'Indicator Name', 'Short definition',
        'Long definition', 'Source', 'Aggregation method', 'Statistical concept and methodology',
        'Limitations and exceptions', and 'General comments'.
        This function uses pandas merge operation which is more efficient than updating rows in a loop.
    """
    # Load metadata from the provided Excel file
    metadata_columns = [
        "Code",
        "Short definition",
        "Long definition",
        "Source",
        "Aggregation method",
        "Statistical concept and methodology",
        "Limitations and exceptions",
        "General comments",
    ]
    metadata_df = pd.read_excel(path_to_file, usecols=metadata_columns)

    # Rename columns to lower case with underscores for consistency
    metadata_df.columns = [col.lower().replace(" ", "_") for col in metadata_df.columns]

    # Rename the 'code' column to 'wb_seriescode' to match the key in the input DataFrame
    metadata_df.rename(columns={"code": "wb_seriescode"}, inplace=True)

    # Merge the input DataFrame with the metadata DataFrame
    enriched_df = pd.merge(df, metadata_df, on="wb_seriescode", how="left")

    return enriched_df


if __name__ == "__main__":
    main()
