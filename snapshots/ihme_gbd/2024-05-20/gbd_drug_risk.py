"""Script to create a snapshot of dataset.

To get the data follow the following steps:

Important - You need and account to access the data.

*** This step shouldn't need to be run in future updates, it is only needed this time as the drug use data was incorrect in the original import, IHME have since corrected it so we download it here.
* Go to: https://vizhub.healthdata.org/gbd-results/
* In 'GBD Estimate' select 'Risk factor'
* In Measure select 'Deaths' and 'DALYs'
* In Metric select 'Number', 'Percent' and 'Rate'
* In Cause select:
#    - Total All Causes
* In Risk  select 'Drug use'
* In Location select 'Global', 'Select all countries and territories', each of the regions in the following groups: 'WHO region', 'World Bank Income Level' and 'World Bank Regions'
* In Age select:
#    - All Ages
#    - Age-standardized
#    - Under 5
#    - 5-14 years
#    - 15-49 years
#    - 50-69 years
#    - 70+
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
BASE_URL = "https://dl.healthdata.org:443/gbd-api-2021-public/8d08635a0caed3666bea7e9cdfc90786_files/IHME-GBD_2021_DATA-8d08635a-"
NUMBER_OF_FILES = 1


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"ihme_gbd/{SNAPSHOT_VERSION}/gbd_drug_risk.feather")
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


if __name__ == "__main__":
    main()
