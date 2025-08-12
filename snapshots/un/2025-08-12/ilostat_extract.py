"""
DATA EXTRACTION FOR THE WORLD BANK POVERTY AND INEQUALITY PLATFORM (PIP) API

This code generates ILOSTAT data for all countries and aggregations in a combined file

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

import pandas as pd
import requests
from botocore.exceptions import ClientError
from joblib import Memory
from owid.catalog.s3_utils import connect_r2_cached
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


def run() -> None:
    df_toc = ilostat_table_of_contents()

    extract_all_files_and_concatenate(df_toc)


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
        Path(f"{CACHE_DIR}/ilostat_data").mkdir(parents=True, exist_ok=True)
        # Save to parquet
        df.to_parquet(
            f"{CACHE_DIR}/ilostat_data/ilostat_country_{country}.parquet",
            index=False,
        )

    log.info(f"Country data extracted for {country}")

    return df


def ilostat_table_of_contents(download: bool = True) -> pd.DataFrame:
    """
    Get the table of contents from ILOSTAT
    This table contains the list of countries and level of detail (annual, quarterly, monthly)
    """

    url = "https://rplumber.ilo.org/metadata/toc/ref_area/?lang=en&type=code&format=.parquet"
    df = fetch_file(url)

    if download:
        # make sure the directory exists. If not, create it
        Path(f"{CACHE_DIR}/ilostat_data").mkdir(parents=True, exist_ok=True)
        # Save to parquet
        df.to_parquet(
            f"{CACHE_DIR}/ilostat_data/ilostat_table_of_contents.parquet",
            index=False,
        )

    log.info("Table of contents extracted")

    return df


def extract_all_files_and_concatenate(df_toc: pd.DataFrame) -> None:
    """
    Extract all files listed in the table of contents and concatenate them into a single DataFrame.
    """

    # Filter only for freq=A annual
    df_toc = df_toc[df_toc["freq"] == "A"].reset_index(drop=True)

    # Get a list of countries from the id column
    countries = list(df_toc["id"].unique())

    dfs = []
    for country in tqdm(countries, desc="Extracting data from countries"):
        df = ilostat_query_country(country)
        dfs.append(df)

    # Concatenate all these files into a unique one
    df = pd.concat(dfs, ignore_index=True)

    # Export file as csv and parquet
    df.to_parquet(f"{PARENT_DIR}/ilostat_data.parquet", index=False)

    log.info("All files extracted and concatenated")

    return None


if __name__ == "__main__":
    run()
