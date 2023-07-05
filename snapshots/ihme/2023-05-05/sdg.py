"""Script to create a snapshot of dataset 'Sustainable Development Goals (IHME, 2022)'."""

import json
from os import environ as env
from pathlib import Path
from typing import List

import click
import pandas as pd
import requests
from dotenv import load_dotenv
from owid.datautils.io import df_to_file

from etl.paths import BASE_DIR
from etl.snapshot import Snapshot

SNAPSHOT_VERSION = Path(__file__).parent.name
ENV_FILE = env.get("ENV", BASE_DIR / ".env")


load_dotenv(ENV_FILE)

API_KEY = env.get("IHME_SDG_API_KEY")


def get_indicator_ids(api_key: str) -> List[int]:
    """
    Accessing the list of SDG Indicator IDs that IHME has data for.
    """
    response = requests.get("https://api.healthdata.org/sdg/v1/GetIndicator", headers={"Authorization": api_key})
    assert response.ok
    indicator_content = json.loads(response.content)
    indicator_ids = [indicator["indicator_id"] for indicator in indicator_content["results"]]

    return indicator_ids


def get_indicator_data(api_key: str, indicator_ids: List[int]) -> pd.DataFrame:
    """
    For each Indicator ID, fetch all the available data from the IHME API.
    """
    all_indicator_df = pd.DataFrame()
    for indicator in indicator_ids:
        response = requests.get(
            f"https://api.healthdata.org/sdg/v1/GetResultsByIndicator?indicator_id={indicator}",
            headers={"Authorization": api_key},
        )
        indicator_data = json.loads(response.content)
        indicator_df = pd.json_normalize(indicator_data["results"])
        all_indicator_df = pd.concat([all_indicator_df, indicator_df])

    return all_indicator_df


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
def main(upload: bool) -> None:
    assert API_KEY is not None, "Get API key from https://api.healthdata.org/sdg/v1/doc in order to access this data"
    indicator_ids = get_indicator_ids(API_KEY)
    ihme_sdg_df = get_indicator_data(API_KEY, indicator_ids)
    # Create a new snapshot.
    snap = Snapshot(f"ihme/{SNAPSHOT_VERSION}/sdg.csv")

    # Download data from source.
    df_to_file(ihme_sdg_df, file_path=snap.path)

    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
