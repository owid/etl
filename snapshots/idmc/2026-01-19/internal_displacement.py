"""Script to create a snapshot of dataset."""

import os
from time import sleep

import pandas as pd
import requests
import structlog

from etl.helpers import PathFinder

paths = PathFinder(__file__)

IDMC_TOKEN = os.getenv("IDMC_API_TOKEN")
IDMC_API_BASE_URL = "https://helix-tools-api.idmcdb.org"

LOG = structlog.get_logger()


def run(upload: bool = True) -> None:
    """Create a new snapshot.

    Args:
        upload: Whether to upload the snapshot to S3.
    """
    # Init Snapshot object
    snap = paths.init_snapshot()

    limit = 100

    # Download data from API.
    combined_url = (
        f"{IDMC_API_BASE_URL}/external-api/gidd/displacements/?client_id={IDMC_TOKEN}&format=json&limit={limit}"
    )

    LOG.info("Downloading data from IDMC API")
    combined_df = data_from_api(combined_url)

    # Save snapshot.
    snap.create_snapshot(upload=upload, data=combined_df)


def data_from_api(url: str) -> pd.DataFrame:
    """Download data from API and return it as a pd.DataFrame

    Args:
        url: The API endpoint URL.
        filename: The filename to save the data to.
    """
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    while data.get("next"):
        response = requests.get(data["next"])
        response.raise_for_status()
        next_data = response.json()
        data["results"].extend(next_data["results"])
        data["next"] = next_data["next"]
        sleep(1)  # To avoid hitting rate limits

    data_df = pd.DataFrame(data["results"])

    return data_df
