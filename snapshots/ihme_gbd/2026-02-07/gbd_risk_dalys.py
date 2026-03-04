"""Script to create a snapshot of dataset.

To get the data follow the following steps:

Important - You need and account to access the data.

* Go to: https://vizhub.healthdata.org/gbd-results/
* In 'GBD Estimate' select 'Risk factor'
* In Measure select 'DALYs'
* In Metric select 'Number',  and 'Rate'
* In Cause select:
#    - All Causes
* In Risk  select:
    - Unsafe water source
    - Unsafe sanitation
    - Air pollution
    - Ambient particulate matter pollution
    - Household air pollution from solid fuels
    - Lead exposure
    - Iron deficiency
    - Vitamin A deficiency
    - Zinc deficiency
    - Smoking
    - Secondhand smoke
    - Drug use
    - High-fasting plasma glucose
    - High systolic blood pressure
    - High body mass index
    - Diet low in fruits
    - Diet low in vegetables
    - Diet high in sodium
    - Low physical activity
    - Non-exclusive breastfeeding
    - Child wasting
    - Child stunting
    - High LDL cholesterol
    - Particulate matter pollution
* In Location select 'Global', 'Select all countries and territories', each of the regions in the following groups: 'WHO region', 'World Bank Income Level' and 'World Bank Regions'
* In Age select:
#    - All Ages
#    - Age-standardized
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
BASE_URL = "https://dl.healthdata.org/gbd-api-2023-collaborator/da7b1c3057b7f2ca70b1a666b756498f_files/IHME-GBD_2023_DATA-da7b1c30-"
NUMBER_OF_FILES = 2


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"ihme_gbd/{SNAPSHOT_VERSION}/gbd_risk_dalys.feather")
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
