"""Script to create a snapshot of dataset."""

import time
from datetime import datetime as dt
from io import StringIO
from pathlib import Path

import click
import pandas as pd
import requests
from structlog import get_logger
from tqdm.auto import tqdm

from etl.snapshot import Snapshot

logger = get_logger()

CT_BASE_URL = "https://beta-ut.clinicaltrials.gov/api/v2/studies"

CT_SIZE_URL = "https://beta-ut.clinicaltrials.gov/api/v2/stats/size"

PAGE_SIZE = 1000
FORMAT = "csv"

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


def fetch_full_clinical_trials_data(page_size: int) -> list:
    chunks = []
    total_size = fetch_clinical_trials_size()
    logger.info("Total clinical trials", total_size=total_size)
    next_page_token = None
    pbar = tqdm(total=total_size, desc="Fetching data", unit="entries")
    while True:
        data_txt, next_page_token = fetch_next_page_ct(page_size, next_page_token=next_page_token)
        chunks.append(data_txt)
        pbar.update(page_size)
        if not next_page_token:
            break
        time.sleep(0.1)  # To avoid hitting rate limits
    pbar.close()
    return chunks


def fetch_clinical_trials_size() -> int:
    response = requests.get(CT_SIZE_URL)

    if response.status_code == 200:
        size_data = response.json()
        return size_data.get("totalStudies", None)
    else:
        raise Exception(f"Error fetching size data: {response.status_code} - {response.text}")


def fetch_next_page_ct(page_size, next_page_token=None):
    params = {"pageSize": page_size, "pageToken": next_page_token, "format": FORMAT}

    response = requests.get(CT_BASE_URL, params=params)

    if response.status_code == 200:
        return response.text, response.headers.get("x-next-page-token", None)
    else:
        raise Exception(f"Error fetching data: {response.status_code} - {response.text}")


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def run(upload: bool) -> None:
    # Initialize a new snapshot.
    snap = Snapshot(f"health/{SNAPSHOT_VERSION}/clinical_trials.csv")

    logger.info("Fetching data from ClinicalTrials...", API_URL=CT_BASE_URL)
    chunks = fetch_full_clinical_trials_data(PAGE_SIZE)

    full_tb = pd.read_csv(StringIO("".join(chunks)))

    # Save snapshots.
    pct = dt.now()
    print("current time:", pct)
    print(f"Saving snapshot to {snap.path}...")
    snap.create_snapshot(data=full_tb, upload=upload)


if __name__ == "__main__":
    run()
