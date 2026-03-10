"""
ILOSTAT data extraction script.

This script extracts data from the ILOSTAT API and uploads snapshots directly.

To run this code:
    1. (Optional) Connect to the staging server:
        - Hit Cmd + Shift + P and select Remote-SSH: Connect to Host
        - Type in owid@staging-site-{branch_name}
    2. (Optional) Delete the local cache folder:
        rm -rf .cache/ilostat_data
    3. (Optional) Delete the R2 cache:
        rclone delete r2:owid-private/cache/ilostat/ --fast-list --transfers 32 --checkers 32 --verbose
    4. Run the script:
        etls un/{version}/ilostat_extract
        # Or in background:
        nohup uv run etls un/{version}/ilostat_extract > output_ilostat.log 2>&1 &
    5. Kill the process if needed:
        pkill -f ilostat_extract

The entire script takes about 20 minutes.

Snapshots created:
    - ilostat.parquet (main data)
    - ilostat_dictionary_*.parquet (dimension dictionaries)
    - ilostat_table_of_contents_country.parquet (country metadata)
"""

import io
from pathlib import Path
from typing import Literal

import click
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
from etl.snapshot import Snapshot

log = get_logger()

memory = Memory(CACHE_DIR, verbose=0)

SNAPSHOT_VERSION = Path(__file__).parent.name

# API parameters
MAX_REPEATS = 15
TIMEOUT = 500

# Indicators to extract
INDICATORS = [
    "SDG_0111_SEX_AGE_RT",  # SDG indicator 1.1.1 - Working poverty rate (percentage of employed living below US$3 PPP) (%)
    "SDG_0131_SEX_SOC_RT",  # SDG indicator 1.3.1 - Proportion of population covered by social protection floors/systems (%)
    "SDG_0552_NOC_RT",  # SDG indicator 5.5.2 - Proportion of women in senior and middle management positions (%)
    "SDG_T552_NOC_RT",  # SDG indicator 5.5.2 - Proportion of women in managerial positions (%)
    "SDG_0821_NOC_RT",  # SDG indicator 8.2.1 - Annual growth rate of output per worker (GDP constant 2021 international $ at PPP) (%)
    "SDG_0831_SEX_ECO_RT",  # SDG indicator 8.3.1 - Proportion of informal employment in total employment
    "SDG_0851_SEX_OCU_NB",  # SDG indicator 8.5.1 - Average hourly earnings of employees
    "SDG_0852_SEX_AGE_RT",  # SDG indicator 8.5.2 - Unemployment rate (%)
    "SDG_0852_SEX_DSB_RT",  # SDG indicator 8.5.2 - Unemployment rate by disability status (%)
    "SDG_0861_SEX_RT",  # SDG indicator 8.6.1 - Proportion of youth (aged 15-24 years) not in education, employment or training
    "SDG_B871_SEX_AGE_RT",  # SDG indicator 8.7.1 - Proportion of children engaged in economic activity and household chores
    "SDG_A871_SEX_AGE_RT",  # SDG indicator 8.7.1 - Proportion of children engaged in economic activity (%)
    "SDG_N881_SEX_MIG_RT",  # SDG indicator 8.8.1 - Non-fatal occupational injuries per 100'000 workers
    "SDG_F881_SEX_MIG_RT",  # SDG indicator 8.8.1 - Fatal occupational injuries per 100'000 workers
    "SDG_0882_NOC_RT",  # SDG indicator 8.8.2 - Level of national compliance with labour rights (freedom of association and collective bargaining)
    "SDG_08B1_NOC_NB",  # SDG indicator 8.b.1: Existence of a developed and operationalized national strategy for youth employment
    "SDG_0922_NOC_RT",  # SDG indicator 9.2.2 - Manufacturing employment as a proportion of total employment (%)
    "SDG_1041_NOC_RT",  # SDG indicator 10.4.1 - Labour income share as a percent of GDP (%)
    "EAR_4HRL_SEX_CUR_NB",  # Average hourly earnings of employees by sex and currency
    "EAR_XFLS_NOC_RT",  # Female share of low pay earners (%)
    "EAR_GGAP_OCU_RT",  # Gender wage gap by occupation (%)
    "EAP_2EAP_SEX_AGE_NB",  # Labour force by sex and age -- ILO modelled estimates
    "EAP_2WAP_SEX_AGE_RT",  # Labour force participation rate by sex and age -- ILO modelled estimates
    "EMP_2EMP_SEX_STE_NB",  # Employment by sex and status in employment -- ILO modelled estimates (thousands)
    "EMP_2IFL_SEX_RT",  # Informal employment rate by sex -- ILO modelled estimates
    "UNE_2EAP_SEX_AGE_RT",  # Unemployment rate by sex and age -- ILO modelled estimates (%)
    "CLD_XCHL_SEX_AGE_RT",  # Share of children in child labour by sex and age (%)
]

# Dictionaries to extract
DictionaryType = Literal[
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

DICTIONARIES: list[DictionaryType] = [
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


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload snapshots to S3")
def main(upload: bool) -> None:
    """Extract ILOSTAT data and upload snapshots."""
    # Export dictionaries
    log.info("Extracting dictionaries...")
    for dictionary in DICTIONARIES:
        df = fetch_dictionary(dictionary)
        snap = Snapshot(f"un/{SNAPSHOT_VERSION}/ilostat_dictionary_{dictionary}.parquet")
        snap.create_snapshot(data=df, upload=upload)
        log.info(f"Uploaded dictionary: {dictionary}")

    # Export table of contents for countries
    log.info("Extracting table of contents...")
    df_toc = fetch_table_of_contents("country")
    snap = Snapshot(f"un/{SNAPSHOT_VERSION}/ilostat_table_of_contents_country.parquet")
    snap.create_snapshot(data=df_toc, upload=upload)
    log.info("Uploaded table of contents")

    # Extract and concatenate all country data
    log.info("Extracting country data...")
    df = extract_all_country_data()

    # Filter to selected indicators
    df = filter_indicators(df)

    # Upload main data snapshot
    snap = Snapshot(f"un/{SNAPSHOT_VERSION}/ilostat.parquet")
    snap.create_snapshot(data=df, upload=upload)
    log.info("Uploaded main data snapshot")

    log.info("All snapshots created successfully!")


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
    """Fetch a parquet file from URL with R2 caching."""
    r2 = connect_r2_cached()
    r2_bucket = "owid-private"
    r2_key = "cache/ilostat/" + checksum_str(url)

    # Try to get from R2 cache
    try:
        obj = r2.get_object(Bucket=r2_bucket, Key=r2_key)
        data = obj["Body"].read()
        if b"Server Error" not in data:
            df = pd.read_parquet(io.BytesIO(data))
            log.info("fetch_file.cache_hit", url=url)
            return df
        else:
            log.info("fetch_file.cache_with_error", url=url)
    except ClientError:
        pass

    # Fetch from API
    response = _get_request(url)

    # Save to R2 cache
    r2.put_object(
        Body=response.content,
        Bucket=r2_bucket,
        Key=r2_key,
    )

    df = pd.read_parquet(io.BytesIO(response.content))
    return df


def fetch_country_data(country: str) -> pd.DataFrame:
    """Fetch data for a single country from ILOSTAT API."""
    url = f"https://rplumber.ilo.org/data/ref_area/?id={country}&type=code&format=.parquet"
    df = fetch_file(url)
    log.info(f"Extracted data for {country}")
    return df


def fetch_table_of_contents(type: Literal["country", "indicator"]) -> pd.DataFrame:
    """Fetch table of contents from ILOSTAT."""
    if type == "country":
        url = "https://rplumber.ilo.org/metadata/toc/ref_area/?lang=en&type=code&format=.parquet"
    else:
        url = "https://rplumber.ilo.org/metadata/toc/indicator/?lang=en&format=.parquet"

    df = fetch_file(url)
    log.info(f"Extracted table of contents: {type}")
    return df


def fetch_dictionary(type: DictionaryType) -> pd.DataFrame:
    """Fetch dictionary tables from ILOSTAT."""
    url = f"https://rplumber.ilo.org/metadata/dic/?var={type}&lang=en&format=.parquet"
    df = fetch_file(url)
    log.info(f"Extracted dictionary: {type}")
    return df


def extract_all_country_data() -> pd.DataFrame:
    """Extract data for all countries and concatenate into a single DataFrame."""
    # Check if cached full data exists
    cache_path = Path(CACHE_DIR) / "ilostat_data" / "ilostat_data_full.parquet"
    if cache_path.exists():
        log.info("Loading cached full data from disk")
        return pd.read_parquet(cache_path)

    # Get list of countries (annual data only)
    df_toc = fetch_table_of_contents("country")
    df_toc = df_toc[df_toc["freq"] == "A"].reset_index(drop=True)
    countries = list(df_toc["id"].unique())

    # Fetch data for each country
    dfs = []
    for country in tqdm(countries, desc="Extracting country data"):
        df = fetch_country_data(country)
        dfs.append(df)

    # Concatenate all data
    df = pd.concat(dfs, ignore_index=True)
    assert isinstance(df, pd.DataFrame)

    # Repack for optimal storage
    df = repack_frame(df)

    # Cache to disk
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(cache_path, index=False)
    log.info("Full data extracted and cached")

    return df


def filter_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Filter DataFrame to selected indicators only."""
    length_before = len(df)

    # Verify all expected indicators exist
    missing = set(INDICATORS) - set(df["indicator"].unique())
    if missing:
        log.error("Missing expected indicators", missing_indicators=missing)
        raise ValueError(f"Missing indicators: {missing}")

    # Filter to selected indicators
    df = df[df["indicator"].isin(INDICATORS)].reset_index(drop=True)

    length_after = len(df)
    log.info(f"Filtered from {length_before:,} to {length_after:,} rows")

    return df


if __name__ == "__main__":
    main()
