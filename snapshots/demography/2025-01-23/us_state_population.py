"""Script to create a snapshot of dataset.
To access this data you need to register with FRED and get a free API key,
which you can do here: https://fredaccount.stlouisfed.org/apikeys

"""

from os import environ as env
from pathlib import Path

import click
import pandas as pd
import requests
from dotenv import load_dotenv
from owid.datautils.io import df_to_file

from etl.paths import BASE_DIR
from etl.snapshot import Snapshot

ENV_FILE = env.get("ENV", BASE_DIR / ".env")
load_dotenv(ENV_FILE, override=True)
API_KEY = env.get("FRED_API_KEY")
# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


def download_state_level_population_data() -> pd.DataFrame:
    # State abbreviations
    states = [
        "AL",
        "AK",
        "AZ",
        "AR",
        "CA",
        "CO",
        "CT",
        "DE",
        "FL",
        "GA",
        "HI",
        "ID",
        "IL",
        "IN",
        "IA",
        "KS",
        "KY",
        "LA",
        "ME",
        "MD",
        "MA",
        "MI",
        "MN",
        "MS",
        "MO",
        "MT",
        "NE",
        "NV",
        "NH",
        "NJ",
        "NM",
        "NY",
        "NC",
        "ND",
        "OH",
        "OK",
        "OR",
        "PA",
        "RI",
        "SC",
        "SD",
        "TN",
        "TX",
        "UT",
        "VT",
        "VA",
        "WA",
        "WV",
        "WI",
        "WY",
    ]
    dataframes = pd.DataFrame()
    for state in states:
        r = requests.get(
            f"https://api.stlouisfed.org/fred/series/observations?series_id={state}POP&api_key={API_KEY}&file_type=json"
        )
        assert r.ok
        data = r.json()
        # Extract the 'observations' section
        observations = data["observations"]
        # Convert to a Pandas DataFrame
        df = pd.DataFrame(observations)
        df["state"] = state
        dataframes = pd.concat([dataframes, df])

    return dataframes


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.

    snap = Snapshot(f"demography/{SNAPSHOT_VERSION}/us_state_population.csv")
    pop_data = download_state_level_population_data()
    df_to_file(pop_data, file_path=snap.path)
    # Download data from source, add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
