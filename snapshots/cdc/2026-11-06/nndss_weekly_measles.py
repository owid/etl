"""Script to create a snapshot of dataset.

Weekly provisional measles cases (imported and indigenous) by reporting area, from the NNDSS Weekly Data
on data.cdc.gov (https://data.cdc.gov/NNDSS/NNDSS-Weekly-Data/x9gk-5huc).
"""

from pathlib import Path
from urllib.parse import urlencode

import pandas as pd

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name
URL_API = "https://data.cdc.gov/resource/x9gk-5huc.csv"


def download_data() -> pd.DataFrame:
    params = {"$where": "label like 'Measles%'", "$limit": 1_000_000, "$order": "sort_order"}
    df = pd.read_csv(f"{URL_API}?{urlencode(params)}")
    assert len(df) < params["$limit"], "Results may be truncated; raise the limit."
    return df


def run(upload: bool = True) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"cdc/{SNAPSHOT_VERSION}/nndss_weekly_measles.csv")

    # Download data, add file to DVC and upload to S3.
    snap.create_snapshot(data=download_data(), upload=upload)
