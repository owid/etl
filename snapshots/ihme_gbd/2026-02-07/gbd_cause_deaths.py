"""Script to create a snapshot of dataset.

To get the data follow the following steps:

Important - You need and account to access the data.

* Go to: https://vizhub.healthdata.org/gbd-results/
* In 'GBD Estimate' select 'Cause of death or injury'
* In Measure select 'Deaths'
* In Metric select 'Number', 'Percent and 'Rate'
* In Cause select 'Select all causes'
* In Location select 'Global', 'Select all countries and territories', each of the regions in the following groups: 'WHO region', 'World Bank Income Level' and 'World Bank Regions'
* In Age select 'All ages', 'Age-standardized', '<5 years', '5-14 years', '15-49 years', '50-69 years', '70+ years'
* In Sex select 'Both'
* In Year select 'Select all'

The data will then be requested and a download link will be sent to you with a number of zip files containing the data (approx < 10 files).

We will download and combine the files in the following script.
"""

from pathlib import Path

import click
import pandas as pd
from owid.datautils.dataframes import concatenate
from owid.repack import repack_frame
from shared import download_data  # type: ignore[reportMissingImports]
from structlog import get_logger

from etl.snapshot import Snapshot

log = get_logger()

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name
# The base url is the url given by the IHME website to download the data, with the file number and .zip removed e.g. '1.zip'
BASE_URL = "https://dl.healthdata.org/gbd-api-2023-collaborator/178d50d9d169b74c922690f320c61f0a_files/IHME-GBD_2023_DATA-178d50d9-"
NUMBER_OF_FILES = 68
# The download had to be broken down into two parts due to the number of files, the request was failing when trying to request the full dataset
BASE_URL_TWO = "https://dl.healthdata.org/gbd-api-2023-collaborator/8fcc2fc172242b753401d0cbec450ea8_files/IHME-GBD_2023_DATA-8fcc2fc1-"
NUMBER_OF_FILES_TWO = 29
BASE_URL_THREE = "https://dl.healthdata.org/gbd-api-2023-collaborator/616c2b05e77d03274e527a55f718fd22_files/IHME-GBD_2023_DATA-616c2b05-"
NUMBER_OF_FILES_THREE = 2


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"ihme_gbd/{SNAPSHOT_VERSION}/gbd_cause_deaths.feather")
    # Download data from source.
    dfs: list[pd.DataFrame] = []
    for file_number in range(1, NUMBER_OF_FILES + NUMBER_OF_FILES_TWO + NUMBER_OF_FILES_THREE + 1):
        log.info(f"Downloading file {file_number} of {NUMBER_OF_FILES + NUMBER_OF_FILES_TWO+ NUMBER_OF_FILES_THREE}")
        if file_number <= NUMBER_OF_FILES:
            df = download_data(file_number, base_url=BASE_URL)
        elif file_number <= NUMBER_OF_FILES + NUMBER_OF_FILES_TWO:
            # Downloading the second batch of files
            file_number_two = file_number - NUMBER_OF_FILES
            df = download_data(file_number_two, base_url=BASE_URL_TWO)
        else:
            # Downloading the third batch of files
            file_number_three = file_number - (NUMBER_OF_FILES + NUMBER_OF_FILES_TWO)
            df = download_data(file_number_three, base_url=BASE_URL_THREE)

        log.info(f"Download of file {file_number} finished", size=f"{df.memory_usage(deep=True).sum()/1e6:.2f} MB")
        dfs.append(df)

    # Concatenate the dataframes while keeping categorical columns to reduce memory usage.
    df = repack_frame(concatenate(dfs))

    log.info("Uploading final file", size=f"{df.memory_usage(deep=True).sum()/1e6:.2f} MB")
    snap.create_snapshot(upload=upload, data=df)
