"""
DATA EXTRACTION FOR THE WORLD BANK POVERTY AND INEQUALITY PLATFORM (PIP) API

This code generates ILOSTAT data for all countries and aggregations in a combined file

I modified the code that Mojmir implemented for requests in the PIP API available in the script snapshots/wb/{version}/pip_api.py

To run this code from scratch,
    - (If you want) Connect to the staging server of this pull request:
        - Hit Cmd + Shift + P and select Remote-SSH: Connect to Host
        - Type in owid@staging-site-{branch_name}
    - Delete the files in the local cache folder:
        rm -rf .cache/*
    - (If needed) Delete the files in R2:
        rclone delete r2:owid-private/cache/ilostat/ --fast-list --transfers 32 --checkers 32 --verbose
    - Run the code. You have two options to see the output, in the terminal or in the background:
        python snapshots/un/{version}/ilostat_extract.py
        nohup uv run python snapshots/un/{version}/ilostat_extract.py > output_ilostat.log 2>&1 &
    - You can kill the process with:
        pkill -f ilostat_extract

The entire script, with extraction and file generation takes about 20 minutes.

When the code finishes, you will have the following file in this folder:
    - ilostat_data.parquet

Now you can run
    etls ilostat --path-to-file snapshots/wb/{version}/ilostat_data.parquet

You can delete the file after this.

"""

import io
from pathlib import Path
from typing import Literal

import pandas as pd
import requests
from botocore.exceptions import ClientError
from joblib import Memory
from owid.catalog.s3_utils import connect_r2_cached
from owid.repack import repack_frame
from structlog import get_logger
from tenacity import retry
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_random_exponential
from tqdm import tqdm

from etl.files import checksum_str
from etl.paths import CACHE_DIR

# Initialize logger.
log = get_logger()

memory = Memory(CACHE_DIR, verbose=0)

# Set directory path
PARENT_DIR = Path(__file__).parent.absolute()

# Basic parameters to use in the functions
MAX_REPEATS = 15
TIMEOUT = 500

# Define expected datasets
EXPECTED_DATASETS = [
    "ILOEST",
    "LFS",
    "ILOSDG",
    "GEND",
    "DLMI",
    "YouthSTATS",
    "CHILD",
    "EMI",
    "PROFILES",
    "RURBAN",
    "COND",
    "ILMS",
    "WORK",
    "OSH",
    "PRICES",
    "IRdata",
]

# Define datasets to drop
DATASETS_TO_DROP = [
    "WORK",
    "OSH",
    "PRICES",
    "IRdata",
]


def run() -> None:
    df = extract_all_files_and_concatenate()

    df = exclude_datasets(df)


@retry(wait=wait_random_exponential(multiplier=1), stop=stop_after_attempt(MAX_REPEATS))
def _get_request(url: str) -> requests.Response:
    response = requests.get(url, timeout=TIMEOUT)
    if response.status_code != 200:
        log.info("fetch_file.retry", url=url)
        raise Exception("API timed out")

    if b"Server Error" in response.content:
        raise Exception("API returned server error")

    return response


@memory.cache
def fetch_file(url: str) -> pd.DataFrame:
    r2 = connect_r2_cached()
    r2_bucket = "owid-private"
    r2_key = "cache/ilostat/" + checksum_str(url)

    # try to get it from cache
    try:
        obj = r2.get_object(Bucket=r2_bucket, Key=r2_key)  # type: ignore[reportAttributeAccessIssue]
        data = obj["Body"].read()
        # we might have cached invalid responses, in that case fetch it again
        if b"Server Error" not in data:
            df = pd.read_parquet(io.BytesIO(data))
            log.info("fetch_file.cache_hit", url=url)
            return df
        else:
            log.info("fetch_file.cache_with_error", url=url)
    except ClientError:
        pass

    # log.info("fetch_file.start", url=url)
    response = _get_request(url)
    # log.info("fetch_file.success", url=url, t=response.elapsed.total_seconds())

    # save the result to R2 cache
    r2.put_object(  # type: ignore[reportAttributeAccessIssue]
        Body=response.content,
        Bucket=r2_bucket,
        Key=r2_key,
    )

    df = pd.read_parquet(io.BytesIO(response.content))
    return df


############################################################################################################
# FUNCTIONS


def ilostat_query_country(
    country: str,
    download: bool = True,
) -> pd.DataFrame:
    """
    Query country data from ILOSTAT
    """

    # Build query
    df = fetch_file(f"https://rplumber.ilo.org/data/ref_area/?id={country}&type=code&format=.parquet")

    if download:
        # make sure the directory exists. If not, create it
        Path(f"{CACHE_DIR}/ilostat_data/data_by_country/").mkdir(parents=True, exist_ok=True)
        # Save to parquet
        df.to_parquet(
            f"{CACHE_DIR}/ilostat_data/data_by_country/ilostat_country_{country}.parquet",
            index=False,
        )

    log.info(f"Country data extracted for {country}")

    return df


def ilostat_table_of_contents(type: Literal["country", "indicator"]) -> pd.DataFrame:
    """
    Get the table of contents from ILOSTAT
    This table contains the list of countries and level of detail (annual, quarterly, monthly)
    """
    if type == "country":
        url = "https://rplumber.ilo.org/metadata/toc/ref_area/?lang=en&type=code&format=.parquet"
    elif type == "indicator":
        url = "https://rplumber.ilo.org/metadata/toc/indicator/?lang=en&format=.parquet"

    df = fetch_file(url)

    # make sure the directory exists. If not, create it
    Path(f"{CACHE_DIR}/ilostat_data").mkdir(parents=True, exist_ok=True)

    # Save to parquet
    df.to_parquet(
        f"{CACHE_DIR}/ilostat_data/ilostat_table_of_contents_{type}.parquet",
        index=False,
    )

    log.info(f"Table of contents extracted: {type}")

    return df


def extract_all_files_and_concatenate() -> pd.DataFrame:
    """
    Extract all files listed in the table of contents and concatenate them into a single DataFrame.
    """

    # Check if file in {CACHE_DIR}/ilostat_data_full.parquet" exists
    # It's better to do this check, since the extraction and concatenation takes time
    if Path(f"{CACHE_DIR}/ilostat_data/ilostat_data_full.parquet").exists():
        log.info("File ilostat_data_full.parquet already exists, skipping extraction")

        df = pd.read_parquet(f"{CACHE_DIR}/ilostat_data/ilostat_data_full.parquet")

        return df
    else:
        df_toc_country = ilostat_table_of_contents(type="country")

        # Filter only for freq=A annual
        df_toc_country = df_toc_country[df_toc_country["freq"] == "A"].reset_index(drop=True)

        # Get a list of countries from the id column
        countries = list(df_toc_country["id"].unique())

        dfs = []
        for country in tqdm(countries, desc="Extracting data from countries"):
            df = ilostat_query_country(country)
            dfs.append(df)

        # Concatenate all these files into a unique one
        df = pd.concat(dfs, ignore_index=True)

        # Repack the dataframe to optimize columns
        df = repack_frame(df)

        # Export file as csv and parquet
        df.to_parquet(f"{CACHE_DIR}/ilostat_data/ilostat_data_full.parquet", index=False)

        log.info("Full data extracted and concatenated")

    return df


def exclude_datasets(df: pd.DataFrame) -> pd.DataFrame:
    """
    Exclude unwanted datasets from the DataFrame.
    """

    # Extract table of contents for indicators
    df_toc_indicator = ilostat_table_of_contents(type="indicator")

    # Keep only indicators where freq=A
    df_toc_indicator = df_toc_indicator[df_toc_indicator["freq"] == "A"].reset_index(drop=True)

    # Keep only datasets with non-missing database
    df_toc_indicator = df_toc_indicator[
        (df_toc_indicator["database"] != "NA") & (df_toc_indicator["database"].notna())
    ].reset_index(drop=True)

    # Assert that all expected databases are present, and there is no new dataset
    assert set(EXPECTED_DATASETS).issubset(set(df_toc_indicator["database"].unique())), log.error(
        "Missing expected datasets in the DataFrame",
        missing_datasets=set(EXPECTED_DATASETS).difference(set(df_toc_indicator["database"].unique())),
    )
    assert not set(df_toc_indicator["database"].unique()).difference(set(EXPECTED_DATASETS)), log.error(
        "Unexpected datasets found in the DataFrame",
        unexpected_datasets=set(df_toc_indicator["database"].unique()).difference(set(EXPECTED_DATASETS)),
    )

    # Keep only datasets to drop
    df_toc_indicator = df_toc_indicator[df_toc_indicator["database"].isin(DATASETS_TO_DROP)].reset_index(drop=True)

    # Create a list of the indicators to drop
    indicators_to_drop = list(df_toc_indicator["indicator"].unique())

    # Calculate length of the DataFrame
    length_before = len(df)

    # Drop indicators
    df = df[~df["indicator"].isin(indicators_to_drop)].reset_index(drop=True)

    # Calculate length after
    length_after = len(df)

    log.info(f"Excluded datasets: {DATASETS_TO_DROP}")
    log.info(
        f"Removed {(length_before - length_after):,} rows from the DataFrame. Now there are {length_after:,} rows."
    )

    df.to_parquet(f"{PARENT_DIR}/ilostat.parquet", index=False)

    log.info("DataFrame with excluded datasets saved to ilostat.parquet")

    return df


if __name__ == "__main__":
    run()
