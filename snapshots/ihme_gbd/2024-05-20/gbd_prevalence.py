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
from pathlib import Path

import click
from shared import compress_files, download_data
from structlog import get_logger

from etl.snapshot import Snapshot

log = get_logger()
# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name
# The base url is the url given by the IHME website to download the data, with the file number and .zip removed e.g. '1.zip'
BASE_URL = "https://dl.healthdata.org:443/gbd-api-2021-public/7fe48f68f1956453091ac5de855166b7_files/IHME-GBD_2021_DATA-7fe48f68-"
NUMBER_OF_FILES = 101


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"ihme_gbd/{SNAPSHOT_VERSION}/gbd_prevalence.csv")
    # Download data from source.
    all_file_paths = []
    with tempfile.TemporaryDirectory() as tmpdirname:
        for file_number in range(1, NUMBER_OF_FILES + 1):
            log.info(f"Downloading file {file_number} of {NUMBER_OF_FILES}")
            file_path = download_data(file_number, tmpdirname, base_url=BASE_URL)
            all_file_paths.append(file_path)

        zip_file_path = os.path.join(tmpdirname, "all_csv_files.zip")
        compress_files(all_file_paths, zip_file_path)
        snap.create_snapshot(upload=upload, filename=zip_file_path)


if __name__ == "__main__":
    main()
