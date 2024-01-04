"""Script to create a snapshot of dataset."""

import time
from pathlib import Path

import click
import pandas as pd
import requests
from tqdm import tqdm

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


URL = 'https://www.unoosa.org/oosa/osoindex/waxs-search.json?criteria={"filters":[],"startAt":0,"sortings":[{"fieldName":"object.launch.dateOfLaunch_s1","dir":"desc"}]}'


def get_n_objects():
    data = requests.get(URL).json()
    return data["found"]


def offset_url(offset):
    return URL.replace('"startAt":0', '"startAt":' + str(offset))


def get_rows(offset):
    url = offset_url(offset)
    try:
        data = requests.get(url).json()
    except Exception:
        time.sleep(10)
        data = requests.get(url).json()
    return pd.DataFrame.from_records([result["values"] for result in data["results"]])


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"space/{SNAPSHOT_VERSION}/object_launches.csv")

    # Fetch data
    n = get_n_objects()
    print(f"{n} objects found.")

    data = []
    for i in tqdm(range(0, n + 1, 15)):
        data.append(get_rows(i))

    data = pd.concat(data)
    assert len(data) == n

    # Add file to DVC and upload to S3.
    snap.create_snapshot(data=data, upload=upload)


if __name__ == "__main__":
    main()
