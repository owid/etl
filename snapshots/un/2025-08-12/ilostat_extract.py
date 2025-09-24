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

When the code finishes, you will have the following files in this folder:
    - ilostat.parquet
    - ilostat_dictionary_classif1.parquet
    - ilostat_dictionary_classif2.parquet
    - ilostat_dictionary_indicator.parquet
    - ilostat_dictionary_note_classif.parquet
    - ilostat_dictionary_note_indicator.parquet
    - ilostat_dictionary_note_source.parquet
    - ilostat_dictionary_obs_status.parquet
    - ilostat_dictionary_ref_area.parquet
    - ilostat_dictionary_sex.parquet
    - ilostat_dictionary_source.parquet
    - ilostat_table_of_contents_country.parquet

Now you can run
    etls ilostat --path-to-file snapshots/un/2025-08-12/ilostat.parquet
    etls ilostat_dictionary_classif1 --path-to-file snapshots/un/2025-08-12/ilostat_dictionary_classif1.parquet
    etls ilostat_dictionary_classif2 --path-to-file snapshots/un/2025-08-12/ilostat_dictionary_classif2.parquet
    etls ilostat_dictionary_indicator --path-to-file snapshots/un/2025-08-12/ilostat_dictionary_indicator.parquet
    etls ilostat_dictionary_note_classif --path-to-file snapshots/un/2025-08-12/ilostat_dictionary_note_classif.parquet
    etls ilostat_dictionary_note_indicator --path-to-file snapshots/un/2025-08-12/ilostat_dictionary_note_indicator.parquet
    etls ilostat_dictionary_note_source --path-to-file snapshots/un/2025-08-12/ilostat_dictionary_note_source.parquet
    etls ilostat_dictionary_obs_status --path-to-file snapshots/un/2025-08-12/ilostat_dictionary_obs_status.parquet
    etls ilostat_dictionary_ref_area --path-to-file snapshots/un/2025-08-12/ilostat_dictionary_ref_area.parquet
    etls ilostat_dictionary_sex --path-to-file snapshots/un/2025-08-12/ilostat_dictionary_sex.parquet
    etls ilostat_dictionary_source --path-to-file snapshots/un/2025-08-12/ilostat_dictionary_source.parquet
    etls ilostat_table_of_contents_country --path-to-file snapshots/un/2025-08-12/ilostat_table_of_contents_country.parquet

You can delete the file after this.
    rm -rf snapshots/un/2025-08-12/ilostat.parquet
    rm -rf snapshots/un/2025-08-12/ilostat_dictionary_classif1.parquet
    rm -rf snapshots/un/2025-08-12/ilostat_dictionary_classif2.parquet
    rm -rf snapshots/un/2025-08-12/ilostat_dictionary_indicator.parquet
    rm -rf snapshots/un/2025-08-12/ilostat_dictionary_note_classif.parquet
    rm -rf snapshots/un/2025-08-12/ilostat_dictionary_note_indicator.parquet
    rm -rf snapshots/un/2025-08-12/ilostat_dictionary_note_source.parquet
    rm -rf snapshots/un/2025-08-12/ilostat_dictionary_obs_status.parquet
    rm -rf snapshots/un/2025-08-12/ilostat_dictionary_ref_area.parquet
    rm -rf snapshots/un/2025-08-12/ilostat_dictionary_sex.parquet
    rm -rf snapshots/un/2025-08-12/ilostat_dictionary_source.parquet
    rm -rf snapshots/un/2025-08-12/ilostat_table_of_contents_country.parquet

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

# Define indicators to extract

INDICATORS = [
    "SDG_0111_SEX_AGE_RT",
    "SDG_0131_SEX_SOC_RT",
    "SDG_0552_NOC_RT",
    "SDG_T552_NOC_RT",
    "SDG_0821_NOC_RT",
    "SDG_0831_SEX_ECO_RT",
    "SDG_0851_SEX_OCU_NB",
    "SDG_0852_SEX_AGE_RT",
    "SDG_0852_SEX_DSB_RT",
    "SDG_0861_SEX_RT",
    "SDG_B871_SEX_AGE_RT",
    "SDG_A871_SEX_AGE_RT",
    "SDG_N881_SEX_MIG_RT",
    "SDG_F881_SEX_MIG_RT",
    "SDG_0882_NOC_RT",
    "SDG_08B1_NOC_NB",
    "SDG_0922_NOC_RT",
    "SDG_1041_NOC_RT",
    "EAR_4HRL_SEX_CUR_NB",
    "EAR_XFLS_NOC_RT",
    "EAR_GGAP_OCU_RT",
    "EAP_2EAP_SEX_AGE_NB",
    "EAP_2WAP_SEX_AGE_RT",
    "EMP_2EMP_SEX_STE_NB",
    "EMP_2IFL_SEX_RT",
    "UNE_2EAP_SEX_AGE_RT",
    "CLD_XCHL_SEX_AGE_RT",
]

# Define dictionaries to extract
DICTIONARIES_TO_EXTRACT = [
    "classif1",
    "classif2",
    "indicator",
    "note_classif",
    "note_indicator",
    "note_source",
    "obs_status",
    "ref_area",
    "sex",
    "source",
]


def run() -> None:
    export_dictionaries_and_table_of_contents()

    df = extract_all_files_and_concatenate()

    df = select_indicators(df)


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


def ilostat_dictionary(
    type: Literal[
        "classif1",
        "classif2",
        "indicator",
        "note_classif",
        "note_indicator",
        "note_source",
        "obs_status",
        "ref_area",
        "sex",
        "source",
    ],
) -> pd.DataFrame:
    """
    Get dictionary tables from ILOSTAT
    They provide more information about dimensional columns
    """

    url = f"https://rplumber.ilo.org/metadata/dic/?var={type}&lang=en&format=.parquet"

    df = fetch_file(url)

    # make sure the directory exists. If not, create it
    Path(f"{CACHE_DIR}/ilostat_data").mkdir(parents=True, exist_ok=True)

    # Save to parquet
    df.to_parquet(
        f"{CACHE_DIR}/ilostat_data/ilostat_table_of_contents_{type}.parquet",
        index=False,
    )

    log.info(f"Dictionary extracted: {type}")

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


def select_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Exclude unwanted datasets from the DataFrame.
    """

    # Calculate length of the DataFrame
    length_before = len(df)

    # Assert that all indicators are available in the dataset
    assert set(INDICATORS).issubset(set(df["indicator"].unique())), log.error(
        "Missing expected indicators in the DataFrame",
        missing_indicators=set(INDICATORS).difference(set(df["indicator"].unique())),
    )

    # Keep only the indicators we want
    df = df[df["indicator"].isin(INDICATORS)].reset_index(drop=True)

    # Calculate length after
    length_after = len(df)

    log.info(
        f"Removed {(length_before - length_after):,} rows from the DataFrame. Now there are {length_after:,} rows."
    )

    df.to_parquet(f"{PARENT_DIR}/ilostat.parquet", index=False)

    log.info("DataFrame with excluded selected indicators saved to ilostat.parquet")

    return df


def export_dictionaries_and_table_of_contents() -> None:
    """
    Export dictionaries that expand on the information provided on the main dataset.
    Also export the table of content for countries, because it includes info on the ILO regions and subregions
    """

    # Export dictionaries
    for dictionary in DICTIONARIES_TO_EXTRACT:
        df = ilostat_dictionary(type=dictionary)
        df.to_parquet(f"{PARENT_DIR}/ilostat_dictionary_{dictionary}.parquet", index=False)

    # Export table of contents for countries
    df_toc_country = ilostat_table_of_contents(type="country")
    df_toc_country.to_parquet(f"{PARENT_DIR}/ilostat_table_of_contents_country.parquet", index=False)

    log.info("Dictionaries and table of contents exported successfully")


if __name__ == "__main__":
    run()
