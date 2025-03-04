"""Script to create a snapshot of dataset.

Adapted from Ed's original code.

NOTE: The date_published can be found in:
https://www.unoosa.org/oosa/en/spaceobjectregister/index.html
See "Registration Submissions Update" right above the list of updates.

"""

import time
from pathlib import Path

import click
import pandas as pd
import requests
from tqdm import tqdm

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# URL to fetch data from.
URL = 'https://www.unoosa.org/oosa/osoindex/waxs-search.json?criteria={"filters":[],"startAt":0,"sortings":[{"fieldName":"object.launch.dateOfLaunch_s1","dir":"desc"}]}'


def get_rows(offset):
    url = URL.replace('"startAt":0', '"startAt":' + str(offset))
    try:
        data = requests.get(url).json()
    except Exception:
        time.sleep(10)
        data = requests.get(url).json()
    return pd.DataFrame.from_records([result["values"] for result in data["results"]])


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Initialize a new snapshot.
    snap = Snapshot(f"space/{SNAPSHOT_VERSION}/object_launches.csv")

    # Get number of objects.
    n_objects = requests.get(URL).json()["found"]

    # Fetch data
    data = []
    for i in tqdm(range(0, n_objects + 1, 15)):
        data.append(get_rows(i))
    data = pd.concat(data)
    error = "Fetched data does not match the expected number of objects."
    assert len(data) == n_objects, error

    # Save snapshot.
    snap.create_snapshot(data=data, upload=upload)


if __name__ == "__main__":
    main()
