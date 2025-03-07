"""Script to create a snapshot of dataset.

NOTE: The date_published can be found in:
https://www.unoosa.org/oosa/en/spaceobjectregister/index.html
See "Registration Submissions Update" right above the list of updates.

"""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import click
import pandas as pd
import requests
from tqdm import tqdm

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Base URL to fetch data.
BASE_URL = 'https://www.unoosa.org/oosa/osoindex/waxs-search.json?criteria={"filters":[],"startAt":%d,"sortings":[{"fieldName":"object.launch.dateOfLaunch_s1","dir":"desc"}]}'

# Maximum number of concurrent threads.
MAX_WORKERS = 8


def get_rows(offset, session):
    """Fetch data for a specific offset using a persistent session."""
    url = BASE_URL % offset
    retries = 3
    for attempt in range(retries):
        try:
            response = session.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            return pd.DataFrame.from_records([result["values"] for result in data["results"]])
        except requests.exceptions.RequestException:
            if attempt < retries - 1:
                time.sleep(2**attempt)
            else:
                return pd.DataFrame()


def fetch_data_in_parallel():
    # Create a session for faster, persistent requests.
    with requests.Session() as session:
        # Get total number of objects.
        response = session.get(BASE_URL % 0, timeout=10)
        response.raise_for_status()
        n_objects = response.json()["found"]

        # Generate list of offsets.
        offsets = list(range(0, n_objects + 1, 15))

        # Fetch data in parallel.
        data_frames = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_offset = {executor.submit(get_rows, offset, session): offset for offset in offsets}
            for future in tqdm(as_completed(future_to_offset), total=len(offsets), desc="Fetching data"):
                data_frames.append(future.result())

        # Combine all results.
        data = pd.concat(data_frames, ignore_index=True)

        # Ensure data integrity.
        assert len(data) == n_objects, "Fetched data does not match the expected number of objects."

    return data


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Initialize a new snapshot.
    snap = Snapshot(f"space/{SNAPSHOT_VERSION}/object_launches.csv")

    # Fetch data.
    data = fetch_data_in_parallel()

    # Save snapshot.
    snap.create_snapshot(data=data, upload=upload)


if __name__ == "__main__":
    main()
