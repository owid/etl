"""Script to create a snapshot of dataset."""

import json
from pathlib import Path
from typing import List

import click
import pandas as pd
import requests
from joblib import Memory
from owid.repack import repack_frame
from structlog import get_logger
from tenacity import Retrying
from tenacity.retry import retry_if_exception_type
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_fixed
from tqdm import tqdm

from etl.paths import CACHE_DIR
from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

log = get_logger()

memory = Memory(CACHE_DIR, verbose=0)


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"who/{SNAPSHOT_VERSION}/ghe.feather")

    # Get the list of causes of disease/injury. The data is too big to request all at once.
    causes = get_causes_list()

    # Download the data
    df = download_cause_data(causes)

    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(data=df, upload=upload)


def get_causes_list() -> List[str]:
    url = "https://frontdoor-l4uikgap6gz3m.azurefd.net/DEX_CMS/GHE_FULL?$apply=groupby((DIM_GHECAUSE_TITLE))"
    res = requests.get(url)
    assert res.ok
    value_json = json.loads(res.content)["value"]
    causes = pd.DataFrame.from_records(value_json)["DIM_GHECAUSE_TITLE"].tolist()
    return causes


@memory.cache()
def get_cause_data(url) -> pd.DataFrame:
    resp = requests.get(url)
    if not resp.ok:
        log.error("Failed to download data", url=url, status=resp.status_code, text=resp.text)
        raise ValueError(f"Failed to download data from {url}")
    data_json = resp.json()
    data_df = pd.DataFrame.from_records(data_json["value"])
    return repack_frame(data_df)


def get_cause_data_with_retry(url) -> pd.DataFrame:  # type: ignore
    for attempt in Retrying(
        wait=wait_fixed(5),
        stop=stop_after_attempt(10),
        retry=retry_if_exception_type(
            (
                ValueError,
                requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectionError,
                requests.exceptions.ChunkedEncodingError,
            )
        ),
    ):
        with attempt:
            return get_cause_data(url)


def download_cause_data(causes) -> pd.DataFrame:
    """Request each individual cause and append it. Selecting the following variables:
    Cause, year, country, age group, sex, DALY count, DALY rate per 100k, deaths, deaths per 100k.
    """
    all_data = []
    years = range(2000, 2022)

    for cause in tqdm(causes, desc="Causes"):
        for year in years:
            log.info("Downloading...", cause=cause, year=year)
            # Use this url to download data for just the All Ages category and for the both sexes category
            # url = f"https://frontdoor-l4uikgap6gz3m.azurefd.net/DEX_CMS/GHE_FULL?$filter=DIM_GHECAUSE_TITLE%20eq%20%27{cause}%27%20and%20DIM_SEX_CODE%20eq%20%27BTSX%27and%20DIM_AGEGROUP_CODE%20eq%20%27ALLAges%27&$select=DIM_GHECAUSE_TITLE,DIM_YEAR_CODE,DIM_COUNTRY_CODE,DIM_AGEGROUP_CODE,DIM_SEX_CODE,VAL_DALY_COUNT_NUMERIC,VAL_DALY_RATE100K_NUMERIC,VAL_DEATHS_COUNT_NUMERIC,VAL_DEATHS_RATE100K_NUMERIC,FLAG_LEVEL"
            # Use this url to download data for all age groups and sexes
            select_cols = [
                "DIM_GHECAUSE_TITLE",
                "DIM_YEAR_CODE",
                "DIM_COUNTRY_CODE",
                "DIM_AGEGROUP_CODE",
                "DIM_SEX_CODE",
                "VAL_DALY_COUNT_NUMERIC",
                "VAL_DALY_RATE100K_NUMERIC",
                "VAL_DTHS_COUNT_NUMERIC",
                "VAL_DTHS_RATE100K_NUMERIC",
                "FLAG_LEVEL",
            ]
            url = f"https://frontdoor-l4uikgap6gz3m.azurefd.net/DEX_CMS/GHE_FULL?$filter=DIM_GHECAUSE_TITLE%20eq%20%27{cause}%27%20and%20DIM_YEAR_CODE%20eq%20{year}&$select={','.join(select_cols)}"
            df = get_cause_data_with_retry(url)
            all_data.append(df)
    # combine dataframes - repack them to make them smaller e.g. use categories where possible. Reset index necessary to save as feather.
    all_df = pd.concat(all_data)
    all_df = all_df.reset_index()

    return repack_frame(all_df)


if __name__ == "__main__":
    main()
