"""This script should be manually adapted and executed on the event of an update of the CAIT datasets.

On the event of a new update, manually check and update all fixed inputs below and associated metadata
file. Then execute this script, which will:
* Download greenhouse gas emissions data from CAIT using Climate Watch Data API.
* Compress the data and upload it to S3 Walden bucket.
* Generate the required metadata, including the md5 hash of the compressed file, and write it to the Walden index.

See https://www.climatewatchdata.org/data-explorer/historical-emissions
"""

import argparse
import gzip
import json
import os
import tempfile
from pathlib import Path
from time import sleep

import requests
from tqdm.auto import tqdm

from owid.walden import Dataset, add_to_catalog

########################################################################################################################

# Fixed inputs.

# CAIT API URL.
CAIT_API_URL = "https://www.climatewatchdata.org/api/v1/data/historical_emissions/"
# Number of records to fetch per api request.
API_RECORDS_PER_REQUEST = 500
# Time to wait between consecutive api requests.
TIME_BETWEEN_REQUESTS = 0.1

########################################################################################################################


def fetch_all_data_from_api(
    api_url=CAIT_API_URL,
    api_records_per_request=API_RECORDS_PER_REQUEST,
    time_between_requests=TIME_BETWEEN_REQUESTS,
):
    """Fetch all CAIT data from Climate Watch Data API.

    Parameters
    ----------
    api_url : str
        API URL.
    api_records_per_request : int
        Maximum number of records to fetch per API request.
    time_between_requests : float
        Time to wait between consecutive API requests.

    Returns
    -------
    data_all : list
        Raw data (list with one dictionary per record).

    """
    # Start requests session.
    session = requests.Session()
    # The total number of records in the database is returned on the header of each request.
    # Send a simple request to get that number.
    response = session.get(url=api_url)
    total_records = int(response.headers["total"])
    print(f"Total number of records to fetch from API: {total_records}")

    # Number of requests to ensure all pages are requested.
    total_requests = round(total_records / api_records_per_request) + 1
    # Collect all data from consecutive api requests. This could be sped up by parallelizing requests.
    data_all = []
    for page in tqdm(range(1, total_requests + 1)):
        response = session.get(url=api_url, json={"page": page, "per_page": api_records_per_request})
        new_data = json.loads(response.content)["data"]
        if len(new_data) == 0:
            print("No more data to fetch.")
            break
        data_all.extend(new_data)
        sleep(time_between_requests)

    return data_all


def save_compressed_data_to_file(data, data_file):
    """Compress data and save it as a gzipped JSON file.

    Parameters
    ----------
    data : list
        Raw data.
    data_file : str
        Path to output file.

    """
    with gzip.open(data_file, "wt", encoding="UTF-8") as _output_file:
        json.dump(data, _output_file)


def main():
    print("Fetching CAIT data from API.")
    api_data = fetch_all_data_from_api()
    metadata = Dataset.from_yaml(Path(__file__).parent / "cait_ghg_emissions.meta.yml")

    with tempfile.TemporaryDirectory() as temp_dir:
        output_file = os.path.join(temp_dir, f"file.json.gz")
        print("Saving fetched data as a compressed temporary file.")

        save_compressed_data_to_file(data=api_data, data_file=output_file)

        add_to_catalog(metadata, output_file, upload=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download greenhouse gas emissions data from CAIT using the Climate Watch Data API, compress and "
        "upload data to S3, and add metadata to Walden index."
    )
    args = parser.parse_args()
    main()
