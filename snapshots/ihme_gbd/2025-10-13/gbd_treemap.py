"""Script to create a snapshot of dataset.

To get the data follow the following steps:

Important - You need and account to access the data.

* Go to: https://vizhub.healthdata.org/gbd-results/
* In 'GBD Estimate' select 'Cause of death or injury'
* In Measure select 'Deaths'
* In Metric select 'Number' and 'Percent'
* In Cause select 'Select all level 2 causes'
* In Location select 'Global', 'China', 'United States', 'Central African Republic', 'Brazil', 'Germany'
* In Age select 'All ages',
* In Sex select 'Both'
* In Year select '2023'

The data will then be requested and a download link will be sent to you with a number of zip files containing the data (approx < 10 files).

We will download and combine the files in the following script.
"""

import time
import zipfile
from io import BytesIO
from pathlib import Path

import click
import pandas as pd
import requests
from owid.datautils.dataframes import concatenate
from owid.repack import repack_frame
from structlog import get_logger

from etl.snapshot import Snapshot

log = get_logger()

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name
# The base url is the url given by the IHME website to download the data, with the file number and .zip removed e.g. '1.zip'
BASE_URL = (
    "https://dl.healthdata.org/gbd-api-2023-public/f6b69ee7efc5bdccb945fec3a1459109_files/IHME-GBD_2023_DATA-f6b69ee7-"
)
NUMBER_OF_FILES = 1


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"ihme_gbd/{SNAPSHOT_VERSION}/gbd_treemap.feather")
    # Download data from source.
    dfs: list[pd.DataFrame] = []
    for file_number in range(1, NUMBER_OF_FILES + 1):
        log.info(f"Downloading file {file_number} of {NUMBER_OF_FILES}")
        df = download_data(file_number, base_url=BASE_URL)
        log.info(f"Download of file {file_number} finished", size=f"{df.memory_usage(deep=True).sum()/1e6:.2f} MB")
        dfs.append(df)

    # Concatenate the dataframes while keeping categorical columns to reduce memory usage.
    df = repack_frame(concatenate(dfs))

    log.info("Uploading final file", size=f"{df.memory_usage(deep=True).sum()/1e6:.2f} MB")
    snap.create_snapshot(upload=upload, data=df)


def download_data(file_number: int, base_url: str) -> pd.DataFrame:
    # Unique URL for each file
    url_to_download = f"{base_url}{file_number}.zip"
    csv_file_name = f"{base_url.split('/')[-1]}{file_number}.csv"

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

    # Load the ZIP file into a BytesIO object
    zip_file = BytesIO(response.content)

    # Read the CSV file from the ZIP file
    with zipfile.ZipFile(zip_file, "r") as z:
        with z.open(csv_file_name) as f:
            df = pd.read_csv(f)
            # Remove columns that end with 'id' except for 'cause_id' and 'rei_id' - these might be useful for hierachical data in the future
            columns_to_keep = [
                col for col in df.columns if not (col.endswith("id") and col not in ["cause_id", "rei_id"])
            ]
            df = df[columns_to_keep]

    # Use smaller types
    df = repack_frame(df)

    return df


if __name__ == "__main__":
    main()
