"""Script to create a snapshot of dataset."""

from pathlib import Path

import click
import pandas as pd
import requests
from tqdm.auto import tqdm

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

API_BASE_URL = "https://www.ngdc.noaa.gov/hazel/hazard-service/api/v1/"


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    for disaster in tqdm(["earthquakes", "tsunamis", "volcanoes"], desc="Disasters"):
        # Create a new snapshot.
        snap = Snapshot(f"noaa_ncei/{SNAPSHOT_VERSION}/natural_hazards_{disaster}.csv")

        # Fetch the first page of the data using the API.
        api_url_disaster = f"{API_BASE_URL}/{disaster}/"
        if disaster == "tsunamis":
            api_url_disaster += "events/"
        response = requests.get(api_url_disaster)

        if not response.ok:
            raise ValueError(f"Failed to fetch data from {api_url_disaster}")

        # Get the number of pages.
        total_pages = response.json()["totalPages"]

        # Store the data of the first page.
        data = response.json()["items"]

        # Fetch the data for all other pages.
        for page in tqdm(range(2, total_pages + 1), desc="Pages"):
            # Request data for current page.
            api_url_disaster_page = f"{api_url_disaster}?page={page}"
            response = requests.get(api_url_disaster_page)
            if not response.ok:
                raise ValueError(f"Failed to fetch data from {API_BASE_URL}")
            # Add data for current page to the data.
            data.extend(response.json()["items"])

        # The following sanity check fails because there are repeated "ids".
        # This happens, at least, for earthquake with id 1926 (Chile 1961), which appears twice.
        # error = f"There are repeated event ids for {disaster}."
        # assert len(data) == len(set([event["id"] for event in data])), error

        # Sanity check.
        error = f"Expected empty response after the last page of {disaster}."
        assert requests.get(f"{api_url_disaster}?page={total_pages + 1}").json()["items"] == [], error

        # Create a dataframe with the data for this disaster.
        df = pd.DataFrame.from_dict(data)

        # Create snapshot for this disaster.
        snap.create_snapshot(upload=upload, data=df)


if __name__ == "__main__":
    main()
