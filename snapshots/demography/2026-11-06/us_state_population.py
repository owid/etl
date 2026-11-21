"""Script to create a snapshot of dataset.

Resident population of each US state and the District of Columbia (in thousands, as of July 1), from FRED.
FRED's graph CSV download needs no API key, but it returns at most a dozen series per request, so we fetch one
state at a time.
"""

import io
from pathlib import Path

import pandas as pd
import requests

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name
URL_FRED = "https://fred.stlouisfed.org/graph/fredgraph.csv"
# FRED state abbreviations (series ids are e.g. ALPOP).
STATES = (
    "AL AK AZ AR CA CO CT DE DC FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE NV NH NJ NM NY NC ND OH OK "
    "OR PA RI SC SD TN TX UT VT VA WA WV WI WY"
).split()


def download_data() -> pd.DataFrame:
    dfs = []
    for state in STATES:
        response = requests.get(URL_FRED, params={"id": f"{state}POP"})
        response.raise_for_status()
        df = pd.read_csv(io.StringIO(response.text)).rename(columns={f"{state}POP": "value"})
        df["state"] = state
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)


def run(upload: bool = True) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"demography/{SNAPSHOT_VERSION}/us_state_population.csv")

    # Download data, add file to DVC and upload to S3.
    snap.create_snapshot(data=download_data(), upload=upload)
