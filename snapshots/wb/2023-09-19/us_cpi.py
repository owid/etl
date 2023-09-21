"""Script to create a snapshot of dataset."""

import xml.etree.ElementTree as ET
from pathlib import Path

import click
import pandas as pd
import requests
from owid.datautils.io import df_to_file

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"wb/{SNAPSHOT_VERSION}/us_cpi.csv")
    url = snap.metadata.origin.url_download  # type: ignore
    df = import_US_cpi_API(url)
    # Save the resulting dataframe to a single csv file
    df_to_file(df, file_path=snap.path)
    # Add the snapshot to DVC
    snap.dvc_add(upload=upload)


def import_US_cpi_API(url):
    """
    Imports the US Consumer Price Index (CPI) data from the World Bank API.
    https://datahelpdesk.worldbank.org/knowledgebase/articles/898581 - visit here for details

    Returns:
        DataFrame: A DataFrame containing the extracted CPI data with columns 'year' and 'fp_cpi_totl'.
                  Returns None if the API request fails.
    """

    # API endpoint URL

    try:
        # Send a GET request to the API endpoint
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for unsuccessful requests

        # Parse the XML response
        root = ET.fromstring(response.content)

        # Initialize lists to store the extracted data
        data = []

        # Iterate over each 'wb:data' element
        for data_elem in root.findall(".//{http://www.worldbank.org}data"):
            # Extract the desired data from the 'wb:data' element
            date_elem = data_elem.find(".//{http://www.worldbank.org}date")
            value_elem = data_elem.find(".//{http://www.worldbank.org}value")

            date = date_elem.text if date_elem is not None else None
            value = value_elem.text if value_elem is not None else None

            if date_elem is not None and value_elem is not None:
                # Append the extracted data to the list
                data.append({"year": date, "fp_cpi_totl": value})
            else:
                print("Year and CPI not found!")
                return
        # Create a DataFrame from the extracted data
        df = pd.DataFrame(data)
        df["year"] = df["year"].astype(int)
        df["fp_cpi_totl"] = df["fp_cpi_totl"].astype(float)
        return df

    except requests.exceptions.RequestException as e:
        # Request was not successful
        print(f"Request failed: {str(e)}")
        return None


if __name__ == "__main__":
    main()
