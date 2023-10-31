"""Script to create a snapshot of dataset."""

import gzip
import json
import tempfile
from pathlib import Path
from time import sleep

import click
import requests
from tqdm.auto import tqdm

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Fixed inputs.

# Climate Watch API URL.
API_URL = "https://www.climatewatchdata.org/api/v1/data/historical_emissions/"
# Number of records to fetch per api request.
API_RECORDS_PER_REQUEST = 500
# Time to wait between consecutive api requests.
TIME_BETWEEN_REQUESTS = 0.1


def fetch_all_data_from_api(
    api_url=API_URL,
    api_records_per_request=API_RECORDS_PER_REQUEST,
    time_between_requests=TIME_BETWEEN_REQUESTS,
):
    """Fetch all data from the Climate Watch Data API.

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


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"climate_watch/{SNAPSHOT_VERSION}/emissions_by_sector.gz")

    # Fetch Climate Watch data from API.
    data = fetch_all_data_from_api()

    # Save data as a compressed temporary file.
    with tempfile.TemporaryDirectory() as temp_dir:
        output_file = Path(temp_dir) / "data.json.gz"

        with gzip.open(output_file, "wt", encoding="UTF-8") as _output_file:
            json.dump(data, _output_file)

        # Add file to DVC and upload to S3.
        snap.create_snapshot(filename=output_file, upload=upload)


if __name__ == "__main__":
    main()
