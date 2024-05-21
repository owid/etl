"""Script to create a snapshot of dataset.

To get the data follow the following steps:

Important - You need and account to access the data.

* Go to: https://vizhub.healthdata.org/gbd-results/
* In 'GBD Estimate' select 'Cause of death or injury'
* In Measure select 'Deaths' and 'DALYs'
* In Metric select 'Number' and 'Rate'
* In Impairment select 'Select all causes'
* In Location select 'Global', 'Select all countries and territories', each of the regions in the following groups: 'WHO region', 'World Bank Income Level' and 'World Bank Regions'
* In Age select 'All ages', 'Age-standardized', '<5 years', '5-14 years', '15-49 years', '50-69 years', '70+ years'
* In Sex select 'Both'
* In Year select 'Select all'

The data will then be requested and a download link will be sent to you with a number of zip files containing the data (approx < 10 files).

We will download and combine the files in the following script.
"""

import os
import tempfile
import time
import zipfile
from pathlib import Path

import click
import pandas as pd
import requests
from owid.datautils.io import df_to_file
from structlog import get_logger

from etl.snapshot import Snapshot

log = get_logger()

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name
# The base url is the url given by the IHME website to download the data, with the file number and .zip removed e.g. '1.zip'
BASE_URL = "https://dl.healthdata.org:443/gbd-api-2021-public/63d75c7559682458eb38f52711fd082b_files/IHME-GBD_2021_DATA-63d75c75-"
NUMBER_OF_FILES = 150


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"ihme_gbd/{SNAPSHOT_VERSION}/gbd_prevalence.csv")
    snap.metadata.is_public = False
    # Download data from source.
    all_dfs = []
    for file_number in range(1, NUMBER_OF_FILES + 1):
        log.info(f"Downloading file {file_number} of {NUMBER_OF_FILES}")
        df = download_data(file_number)
        all_dfs.append(df)

    combined_df = pd.concat(all_dfs, ignore_index=True)
    # Download data from source.
    df_to_file(combined_df, file_path=snap.path)

    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


def download_data(file_number: int) -> pd.DataFrame:
    # Unique URL for each file
    url_to_download = f"{BASE_URL}{file_number}.zip"
    # Retry logic
    max_retries = 5
    backoff_factor = 1  # Factor for exponential backoff

    for attempt in range(max_retries):
        try:
            response = requests.get(url_to_download)
            response.raise_for_status()
            break  # If request is successful, exit the loop
        except requests.RequestException as e:
            if attempt < max_retries - 1:
                wait_time = backoff_factor * (2**attempt)  # Exponential backoff
                print(f"Attempt {attempt + 1} failed: {e}. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print(f"Failed to download the file after {max_retries} attempts. Error: {e}")
                raise
    # Download data from source, open the csv within and return that.
    response = requests.get(url_to_download)
    with tempfile.TemporaryDirectory() as tmpdirname:
        zip_file_name = f"{BASE_URL.split('/')[-1]}{file_number}.zip"
        zip_file_path = os.path.join(tmpdirname, zip_file_name)
        with open(zip_file_path, "wb") as f:
            f.write(response.content)
        # Extract the zip file
        with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
            zip_ref.extractall(tmpdirname)
        # Construct the CSV file name and path
        csv_file_name = f"{BASE_URL.split('/')[-1]}{file_number}.csv"
        csv_file_path = os.path.join(tmpdirname, csv_file_name)
        # Read the CSV file
        df = pd.read_csv(csv_file_path)
        return df


if __name__ == "__main__":
    main()
