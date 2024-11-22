"""
The script fetches education data from the World Bank API and adds metadata for each indicator.
It uses parallel requests to fetch metadata for each indicator largely because previously we were importing all of the data from the database and I'd like to keep the same structure in case we need to expand the number of indicators we import from this database in the future.
At the moment we extract the indicators from the grapher database that are actually used in our charts and then fetch the data and the metadata for only these indicators but this might change in the future."""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import click
import numpy as np
import pandas as pd
import requests
import world_bank_data as wb
from owid.datautils.io import df_to_file
from tqdm import tqdm

from etl.db import get_engine
from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"wb/{SNAPSHOT_VERSION}/edstats.csv")

    # Fetch data from the World Bank API.
    wb_education_df = get_data()

    # Using ThreadPoolExecutor for parallel requests with progress bar. Fetch metadata for each indicator.
    indicators = wb_education_df["wb_seriescode"].unique()
    results = []

    for indicator in indicators:
        result = fetch_indicator_metadata(indicator)
        results.append(result)

    # Create a temporary DataFrame from the results
    temp_df = pd.DataFrame(results, columns=["wb_seriescode", "source_note", "source"])

    # Merge the results back into the original DataFrame
    df = pd.merge(temp_df, wb_education_df, on="wb_seriescode", how="right")
    df_to_file(df, file_path=snap.path)
    # Download data from source, add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


def fetch_indicator_metadata(indicator):
    # Fetch metadata for an indicator
    url = f"https://api.worldbank.org/v2/indicator/{indicator}?format=json"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        # Check if the response contains the expected data structure
        if len(data) > 1 and isinstance(data[1], list) and len(data[1]) > 0:
            source_note = data[1][0].get("sourceNote", np.nan)
            source = data[1][0].get("sourceOrganization", np.nan)
            return indicator, source_note, source
        else:
            print(f"No metadata found for indicator: {indicator}")
            return indicator, np.nan, np.nan
    else:
        print(f"Failed to fetch data for indicator: {indicator}. Status code: {response.status_code}")
        return indicator, np.nan, np.nan


def used_world_bank_ids():
    # This will connect to MySQL from specified ENV, so to run it against production you'd run
    # ETL=.env.prod python snapshots/wb/2024-11-04/edstats.py
    engine = get_engine()
    q = """
    select distinct
        SUBSTRING_INDEX(SUBSTRING(v.descriptionFromProducer, LOCATE('World Bank variable id: ', v.descriptionFromProducer) + LENGTH('World Bank variable id: ')), ' ', 1) AS wb_id,
        v.*
    from chart_dimensions as cd
    join charts as c on c.id = cd.chartId
    join variables as v on v.id = cd.variableId
    where v.datasetId = 6194
    """
    df = pd.read_sql(q, engine)
    df["wb_id"] = df["wb_id"].str.replace(r"\n\nOriginal", "", regex=True)

    return list(df["wb_id"].unique())


def fetch_education_data(education_code: str) -> pd.DataFrame:
    """
    Fetches education data for the given code from the World Bank API.

    Args:
        education_code (str): Education code for fetching data.

    Returns:
        DataFrame: DataFrame with fetched data or None if an error occurs.
    """
    try:
        # Fetch data for the given indicator code
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

    # Add some additional World Bank indicators that aren't used in the charts directly but other datasets use them.
    wb_ids = wb_ids + ["PRJ.ATT.15UP.NED.MF", "SE.ADT.LITR.ZS"]

    # Assert that the list of indicators is not empty
    assert len(wb_ids) > 0, "The list wb_ids is empty after removing None elements."

    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(fetch_education_data, code) for code in wb_ids]
        wb_education = [f.result() for f in tqdm(futures, total=len(wb_ids), desc="Fetching data")]

    # Concatenate all non-empty dataframes efficiently
    wb_education_df = pd.concat(wb_education, ignore_index=True)

    return wb_education_df


if __name__ == "__main__":
    main()
