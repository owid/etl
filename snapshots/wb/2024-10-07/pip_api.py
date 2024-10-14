# TODO: Extract data in arrow files, for faster processing.
# NOTE: Check if we can modify the functions to extract missing percentile series for countries in specific years (Cote d'Ivoire 1998 2008, Montenegro 2013 for example).
"""
DATA EXTRACTION FOR THE WORLD BANK POVERTY AND INEQUALITY PLATFORM (PIP) API

This code generates key indicators and percentiles from the World Bank PIP API.
This is done by combining the results of several queries to the API:
    - A set of poverty lines (8) to obtain key indicators per PPP year (2011, 2017) and for countries and regions.
    - 2298 poverty lines to construct percentiles for a group of countries.
    - 5148 poverty lines to construct percentiles for all the regions.
    - 8217 of poverty lines to construct estimates of relative poverty.

Percentiles are partially constructed because the data officially published by the World Bank is missing some countries and all the regions.

To run this code from scratch,
    - Connect to the staging server of this pull request:
        - Hit Cmd + Shift + P and select Remote-SSH: Connect to Host
        - Type in owid@staging-site-{branch_name}
    - Delete the files in the cache folder:
        rm -rf .cache/*
    - Check if you need to update the poverty lines in the functions `poverty_lines_countries` and `poverty_lines_regions`.
        - Check the list of countries without percentile data. It will show up as a list in the output (These countries are available in a common query but not in the percentile file:)
        - Open
            https://api.worldbank.org/pip/v1/pip?country=LCA&year=all&povline=150&fill_gaps=false&welfare_type=all&reporting_level=all&additional_ind=false&ppp_version=2017&identity=PROD&format=csv
            https://api.worldbank.org/pip/v1/pip-grp?country=OHI&year=all&povline=300&group_by=wb&welfare_type=all&reporting_level=all&additional_ind=false&ppp_version=2017&format=csv
        - And see if any of the `headcount` values is lower than 0.99. If so, you need to add more poverty lines to the functions.
    - Run the code. You have two options to see the output, in the terminal or in the background:
        python snapshots/wb/{version}/pip_api.py
        nohup uv run python snapshots/wb/{version}/pip_api.py > output.log 2>&1 &
    - You can kill the process with:
        pkill -f pip_api

When the code finishes, you will have the following files in the cache folder:
    - world_bank_pip.csv: file with the results of the queries for key indicators (8 for countries and 8 for regions), plus some additional indicators (thresholds, relative poverty).
    - pip_percentiles.csv: file with the percentiles taken from WB Databank and the ones constructed from the API.

Copy these files to this folder and run in the terminal:
    python snapshots/wb/{version}/world_bank_pip.py --path-to-file snapshots/wb/{version}/world_bank_pip.csv
    python snapshots/wb/{version}/world_bank_pip_percentiles.py --path-to-file snapshots/wb/{version}/pip_percentiles.csv

You can delete the files after this.

"""


import io
import time
from multiprocessing.pool import ThreadPool
from pathlib import Path

import click
import numpy as np
import pandas as pd
import requests
from botocore.exceptions import ClientError
from joblib import Memory
from owid.catalog.s3_utils import connect_r2_cached
from structlog import get_logger
from tenacity import retry
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_random_exponential

from etl.files import checksum_str
from etl.paths import CACHE_DIR

# Initialize logger.
log = get_logger()

memory = Memory(CACHE_DIR, verbose=0)

# Basic parameters to use in the functions
MAX_REPEATS = 15
TIMEOUT = 500
FILL_GAPS = "false"
# NOTE: Although the number of workers is set to MAX_WORKERS, the actual number of workers for regional queries is half of that, because the API (`pip-grp`) is less able to handle concurrent requests.
MAX_WORKERS = 2
TOLERANCE_PERCENTILES = 1


# Select live (1) or internal (0) API
LIVE_API = 1


# Constants
def poverty_lines_countries():
    """
    These poverty lines are used to calculate percentiles for countries that are not in the percentile file.
    # We only extract to $150 because the highest P99 not available is St. Lucia
    # NOTE: In future updates, check if these poverty lines are enough for the extraction
    """
    # Define poverty lines and their increase

    under_2_dollars = list(range(1, 200, 1))
    between_2_and_5_dollars = list(range(200, 500, 2))
    between_5_and_10_dollars = list(range(500, 1000, 5))
    between_10_and_20_dollars = list(range(1000, 2000, 10))
    between_20_and_30_dollars = list(range(2000, 3000, 10))
    between_30_and_55_dollars = list(range(3000, 5500, 10))
    between_55_and_80_dollars = list(range(5500, 8000, 10))
    between_80_and_100_dollars = list(range(8000, 10000, 10))
    between_100_and_150_dollars = list(range(10000, 15000, 10))

    # povlines is all these lists together
    povlines = (
        under_2_dollars
        + between_2_and_5_dollars
        + between_5_and_10_dollars
        + between_10_and_20_dollars
        + between_20_and_30_dollars
        + between_30_and_55_dollars
        + between_55_and_80_dollars
        + between_80_and_100_dollars
        + between_100_and_150_dollars
    )

    return povlines


def poverty_lines_regions():
    """
    These poverty lines are used to calculate percentiles for regions. None of them are in the percentile file.
    # We only extract to $300 because the highest P99 not available is Other High Income Countries, with $280
    # NOTE: In future updates, check if these poverty lines are enough for the extraction
    """
    # Define poverty lines and their increase

    under_2_dollars = list(range(1, 200, 1))
    between_2_and_5_dollars = list(range(200, 500, 2))
    between_5_and_10_dollars = list(range(500, 1000, 5))
    between_10_and_20_dollars = list(range(1000, 2000, 10))
    between_20_and_30_dollars = list(range(2000, 3000, 10))
    between_30_and_55_dollars = list(range(3000, 5500, 10))
    between_55_and_80_dollars = list(range(5500, 8000, 10))
    between_80_and_100_dollars = list(range(8000, 10000, 10))
    between_100_and_150_dollars = list(range(10000, 15000, 10))
    between_150_and_175_dollars = list(range(15000, 17500, 10))
    between_175_and_250_dollars = list(range(17500, 25000, 20))
    between_250_and_300_dollars = list(range(25000, 30000, 50))

    # povlines is all these lists together
    povlines = (
        under_2_dollars
        + between_2_and_5_dollars
        + between_5_and_10_dollars
        + between_10_and_20_dollars
        + between_20_and_30_dollars
        + between_30_and_55_dollars
        + between_55_and_80_dollars
        + between_80_and_100_dollars
        + between_100_and_150_dollars
        + between_150_and_175_dollars
        + between_175_and_250_dollars
        + between_250_and_300_dollars
    )

    return povlines


# Define poverty lines for key indicators, depending on the PPP version.
# It includes the international poverty line, lower and upper-middle income lines, and some other lines.
POVLINES_DICT = {
    2011: [100, 190, 320, 550, 1000, 2000, 3000, 4000],
    2017: [100, 215, 365, 685, 1000, 2000, 3000, 4000],
}


PPP_VERSIONS = [2011, 2017]
POV_LINES_COUNTRIES = poverty_lines_countries()
POV_LINES_REGIONS = poverty_lines_regions()

# # DEBUGGING
# PPP_VERSIONS = [2017]
# POV_LINES_COUNTRIES = [1, 1000, 25000, 50000]
# POV_LINES_REGIONS = [1, 1000, 25000, 50000]


@click.command()
@click.option(
    "--live-api/--internal-api",
    default=True,
    type=bool,
    help="Select live (1) or internal (0) API",
)
# @click.option("--path-to-file", prompt=True, type=str, help="Path to local data file.")
def run(live_api: bool) -> None:
    if live_api:
        wb_api = WB_API("https://api.worldbank.org/pip/v1")
    else:
        wb_api = WB_API("https://apiv2qa.worldbank.org/pip/v1")

    # Generate percentiles by extracting the raw files and processing them afterward
    df_percentiles = generate_consolidated_percentiles(generate_percentiles_raw(wb_api), wb_api)

    # Generate relative poverty indicators file
    df_relative = generate_relative_poverty(wb_api)

    # Generate key indicators file and patch medians
    df = generate_key_indicators(wb_api)
    df = median_patch(df, country_or_region="country")

    # Add relative poverty indicators and decile thresholds to the key indicators file
    df = add_relative_poverty_and_decile_thresholds(df, df_relative, df_percentiles, wb_api)


class WB_API:
    def __init__(self, api_address, check_health=False):
        self.api_address = api_address
        self.check_health = check_health

    def health_check(self):
        return pd.read_json(f"{self.api_address}/health-check")[0][0]

    def api_health(self):
        """
        Check if the API is running.
        """
        if not self.check_health:
            return

        # Initialize repeat counter
        repeat = 0

        # health comes from a json containing the status
        health = self.health_check()

        # If the status is different to "PIP API is running", repeat the request until MAX_REPEATS
        while health != "PIP API is running" and repeat < MAX_REPEATS:
            repeat += 1

        if repeat >= MAX_REPEATS:
            # If the status is different to "PIP API is running" after MAX_REPEATS, log fatal error
            raise AssertionError(f"Health check: {health} (repeated {repeat} times)")

    def versions(self):
        return memory.cache(pd.read_csv)(f"{self.api_address}/versions?format=csv")

    def get_table(self, table):
        return pd.read_csv(f"{self.api_address}/aux?table={table}&long_format=false&format=csv")

    def fetch_csv(self, url):
        return _fetch_csv(f"{self.api_address}{url}")


@retry(wait=wait_random_exponential(multiplier=1), stop=stop_after_attempt(MAX_REPEATS))
def _get_request(url: str) -> requests.Response:
    response = requests.get(url, timeout=TIMEOUT)
    if response.status_code != 200:
        log.info("fetch_csv.retry", url=url)
        raise Exception("API timed out")

    if b"Server Error" in response.content:
        raise Exception("API returned server error")

    return response


@memory.cache
def _fetch_csv(url: str) -> pd.DataFrame:
    r2 = connect_r2_cached()
    r2_bucket = "owid-private"
    r2_key = "cache/pip_api/" + checksum_str(url)

    # try to get it from cache
    try:
        obj = r2.get_object(Bucket=r2_bucket, Key=r2_key)  # type: ignore[reportAttributeAccessIssue]
        s = obj["Body"].read().decode("utf-8")
        # we might have cached invalid responses, in that case fetch it again
        if "Server Error" not in s:
            df = pd.read_csv(io.StringIO(s))
            log.info("fetch_csv.cache_hit", url=url)
            return df
        else:
            log.info("fetch_csv.cache_with_error", url=url)
    except ClientError:
        pass

    log.info("fetch_csv.start", url=url)
    response = _get_request(url)
    log.info("fetch_csv.success", url=url, t=response.elapsed.total_seconds())

    # save the result to R2 cache
    r2.put_object(  # type: ignore[reportAttributeAccessIssue]
        Body=response.content,
        Bucket=r2_bucket,
        Key=r2_key,
    )

    df = pd.read_csv(io.StringIO(response.content.decode("utf-8")))
    return df


@memory.cache
def _fetch_percentiles(version: int) -> pd.DataFrame:
    # These URLs were copied from https://datacatalog.worldbank.org/search/dataset/0063646/_poverty_and_inequality_platform_pip_percentiles
    if version == 2011:
        url = "https://datacatalogfiles.worldbank.org/ddh-published/0063646/DR0090357/world_100bin.csv"
    elif version == 2017:
        url = "https://datacatalogfiles.worldbank.org/ddh-published/0063646/DR0090251/world_100bin.csv"
    else:
        raise ValueError(f"Version {version} is not supported")

    _df_percentiles = pd.read_csv(url)

    # # Drop  Unnamed: 0 column (it sometimes appears in the files)
    # _df_percentiles = _df_percentiles.drop(columns=["Unnamed: 0"])

    return _df_percentiles


############################################################################################################
# FUNCTIONS


def pip_aux_tables(wb_api: WB_API, table="all"):
    """
    Download aux tables if the API is running.
    """

    wb_api.api_health()

    if table == "all":
        aux_tables_list = [
            "aux_versions",
            "countries",
            "country_coverage",
            "country_list",
            "cpi",
            "decomposition",
            "dictionary",
            "framework",
            "gdp",
            "incgrp_coverage",
            "indicators",
            "interpolated_means",
            "missing_data",
            "national_poverty_lines",
            "pce",
            "pop",
            "pop_region",
            "poverty_lines",
            "ppp",
            "region_coverage",
            "regions",
            "spl",
            "survey_means",
        ]
        # Create a list of dataframes
        df_dict = {}

        # Download each table and append it to the list
        for tab in aux_tables_list:
            df = wb_api.get_table(tab)

            # Add table to df_dict
            df_dict[tab] = df

    else:
        df = wb_api.get_table(table)

        # Add table to df_dict
        df_dict = {table: df}

    log.info(f'Auxiliary tables downloaded ("{table}")')

    return df_dict


def pip_versions(wb_api) -> dict:
    """
    Download latest PIP data versions if the API is running.
    """

    wb_api.api_health()

    df = wb_api.versions()
    df = df[["ppp_version", "release_version", "version"]]

    # Obtain the max release_version
    max_release_version = df["release_version"].max()

    # Get the version for ppp_versions 2011 and 2017
    versions = df[df["release_version"] == max_release_version]

    # Set index and convert to dict
    versions = versions.set_index("ppp_version", verify_integrity=True).sort_index().to_dict(orient="index")

    version_2011 = versions[2011]["version"]
    version_2017 = versions[2017]["version"]

    log.info(f"PIP dataset versions extracted: 2011 = {version_2011}, 2017 = {version_2017}")

    return versions


def pip_query_country(
    wb_api: WB_API,
    popshare_or_povline,
    value,
    versions,
    country_code="all",
    year="all",
    fill_gaps="true",
    welfare_type="all",
    reporting_level="all",
    ppp_version=2017,
    download="false",
) -> pd.DataFrame:
    """
    Query country data from the PIP API.
    """

    # Test health of the API
    wb_api.api_health()

    # Round povline (popshare) to 2 decimals to work with cents as the minimum unit
    value = round(value, 2)

    # Extract version and release_version from versions dict
    version = versions[ppp_version]["version"]
    release_version = versions[ppp_version]["release_version"]

    # Build query
    df = wb_api.fetch_csv(
        f"/pip?{popshare_or_povline}={value}&country={country_code}&year={year}&fill_gaps={fill_gaps}&welfare_type={welfare_type}&reporting_level={reporting_level}&ppp_version={ppp_version}&version={version}&release_version={release_version}&format=csv"
    )

    # Add PPP version as column
    df["ppp_version"] = ppp_version

    # Replace names of columns and drop redundancies
    df = df.rename(columns={"country_name": "country", "reporting_year": "year"})
    df = df.drop(columns=["region_code"])

    # Reorder columns: ppp_version, country, year, povline and the rest
    first_columns = ["ppp_version", "country", "year", "poverty_line"]
    df = df[first_columns + [column for column in df.columns if column not in first_columns]]

    if download == "true":
        # make sure the directory exists. If not, create it
        Path(f"{CACHE_DIR}/pip_country_data").mkdir(parents=True, exist_ok=True)
        # Save to csv
        df.to_csv(
            f"{CACHE_DIR}/pip_country_data/pip_country_{country_code}_year_{year}_{popshare_or_povline}_{int(round(value*100))}_welfare_{welfare_type}_rep_{reporting_level}_fillgaps_{fill_gaps}_ppp_{ppp_version}.csv",
            index=False,
        )

    if country_code == "all":
        log.info(f"Country data extracted for {popshare_or_povline} = {value} ({ppp_version} PPPs)")
    else:
        log.info(
            f"Country data extracted for {popshare_or_povline} = {value} ({ppp_version} PPPs) in {country_code} (year = {year})"
        )

    return df


def pip_query_region(
    wb_api: WB_API,
    popshare_or_povline,
    value,
    versions,
    country_code="all",
    year="all",
    welfare_type="all",
    reporting_level="all",
    ppp_version=2017,
    download="false",
) -> pd.DataFrame:
    """
    Query regional data from the PIP API.
    """

    # Test health of the API
    wb_api.api_health()

    # Round povline (popshare) to 2 decimals to work with cents as the minimum unit
    value = round(value, 2)

    # Extract version and release_version from versions dict
    version = versions[ppp_version]["version"]
    release_version = versions[ppp_version]["release_version"]

    # Build query
    df = wb_api.fetch_csv(
        f"/pip-grp?{popshare_or_povline}={value}&country={country_code}&year={year}&welfare_type={welfare_type}&reporting_level={reporting_level}&ppp_version={ppp_version}&version={version}&release_version={release_version}&format=csv"
    )

    # Add PPP version as column
    df["ppp_version"] = ppp_version

    # Replace names of columns and drop redundancies
    df = df.rename(columns={"region_name": "country", "reporting_year": "year", "region_code": "country_code"})

    # Reorder columns: ppp_version, country, year, povline and the rest
    first_columns = ["ppp_version", "country", "year", "poverty_line"]
    df = df[first_columns + [column for column in df.columns if column not in first_columns]]

    if download == "true":
        # make sure the directory exists. If not, create it
        Path(f"{CACHE_DIR}/pip_region_data").mkdir(parents=True, exist_ok=True)
        # Save to csv
        df.to_csv(
            f"{CACHE_DIR}/pip_region_data/pip_region_{country_code}_year_{year}_{popshare_or_povline}_{int(round(value*100))}_ppp_{ppp_version}.csv",
            index=False,
        )

    if country_code == "all":
        log.info(f"Regional data extracted for {popshare_or_povline} = {value} ({ppp_version} PPPs)")
    else:
        log.info(
            f"Regional data extracted for {popshare_or_povline} = {value} ({ppp_version} PPPs) in {country_code} (year = {year})"
        )

    return df


# GENERATE PERCENTILES FILES
# This is data not given directly by the query, but we can get it by querying a huge set of poverty lines and assign percentiles according to headcount ratio results.


def generate_percentiles_raw(wb_api: WB_API):
    """
    Generates percentiles data from query results. This is the raw data to get the percentiles.
    Uses concurrent.futures to speed up the process.
    """
    start_time = time.time()

    def get_percentiles_data(povline, versions, ppp_version, country_code):
        """
        Check if country percentiles data exists. If not, run the query.
        """
        if Path(
            f"{CACHE_DIR}/pip_country_data/pip_country_{country_code}_year_all_povline_{povline}_welfare_all_rep_all_fillgaps_{FILL_GAPS}_ppp_{ppp_version}.csv"
        ).is_file():
            return

        else:
            return pip_query_country(
                wb_api,
                popshare_or_povline="povline",
                value=povline / 100,
                versions=versions,
                country_code=country_code,
                year="all",
                fill_gaps=FILL_GAPS,
                welfare_type="all",
                reporting_level="all",
                ppp_version=ppp_version,
                download="true",
            )

    def concurrent_percentiles_function(country_code):
        """
        Executes get_percentiles_data concurrently.
        """
        # Make sure the directory exists. If not, create it
        Path(f"{CACHE_DIR}/pip_country_data").mkdir(parents=True, exist_ok=True)

        with ThreadPool(MAX_WORKERS) as pool:
            tasks = [
                (povline, versions, ppp_version, country_code)
                for ppp_version in PPP_VERSIONS
                for povline in POV_LINES_COUNTRIES
            ]
            pool.starmap(get_percentiles_data, tasks)

    def get_percentiles_data_region(povline, versions, ppp_version):
        """
        Check if region percentiles data exists. If not, run the query.
        """
        if Path(
            f"{CACHE_DIR}/pip_region_data/pip_region_all_year_all_povline_{povline}_ppp_{ppp_version}.csv"
        ).is_file():
            return
        else:
            return pip_query_region(
                wb_api,
                popshare_or_povline="povline",
                value=povline / 100,
                versions=versions,
                country_code="all",
                year="all",
                welfare_type="all",
                reporting_level="all",
                ppp_version=ppp_version,
                download="true",
            )

    def concurrent_percentiles_region_function():
        """
        Executes get_percentiles_data_region concurrently.
        """
        # Make sure the directory exists. If not, create it
        Path(f"{CACHE_DIR}/pip_region_data").mkdir(parents=True, exist_ok=True)
        with ThreadPool(MAX_WORKERS) as pool:
            tasks = [(povline, versions, ppp_version) for ppp_version in PPP_VERSIONS for povline in POV_LINES_REGIONS]
            pool.starmap(get_percentiles_data_region, tasks)

    def get_query_country(povline, ppp_version, country_code):
        """
        Here I check if the country file exists even after the original extraction. If it does, I read it. If not, I start the queries again.
        """
        file_path_country = f"{CACHE_DIR}/pip_country_data/pip_country_{country_code}_year_all_povline_{povline}_welfare_all_rep_all_fillgaps_{FILL_GAPS}_ppp_{ppp_version}.csv"
        if Path(file_path_country).is_file():
            df_query_country = pd.read_csv(file_path_country)
        else:
            # Run the main function to get the data
            log.warning(
                f"We need to come back to the extraction! countries = {country_code}, {povline}, {ppp_version} PPPs)"
            )
            get_percentiles_data(povline, versions, ppp_version, country_code)
            df_query_country = pd.read_csv(file_path_country)

        return df_query_country

    def get_query_region(povline, ppp_version):
        """
        Here I check if the regional file exists even after the original extraction. If it does, I read it. If not, I start the queries again.
        """
        file_path_region = (
            f"{CACHE_DIR}/pip_region_data/pip_region_all_year_all_povline_{povline}_ppp_{ppp_version}.csv"
        )
        if Path(file_path_region).is_file():
            df_query_region = pd.read_csv(file_path_region)
        else:
            # Run the main function to get the data
            log.warning(f"We need to come back to the extraction! regions, {povline}, {ppp_version} PPPs)")
            get_percentiles_data_region(povline, versions, ppp_version)
            df_query_region = pd.read_csv(file_path_region)

        return df_query_region

    def get_list_of_missing_countries():
        """
        Compare the list of countries in a common query (reference file) and the list of countries in the percentile file.
        It generates missing_countries, which is a string with all the elements of the list, in the format for querying multiple countries in the API.
        And also missing_countries_list, which is a list of the countries.
        """
        # Obtain the percentile files the World Bank publishes in their Databank

        df_percentiles_published_2017 = _fetch_percentiles(2017)

        # FOR COUNTRIES
        # Get data from the most common query
        df_reference = pip_query_country(
            wb_api,
            popshare_or_povline="povline",
            value=2.15,
            versions=versions,
            country_code="all",
            year="all",
            fill_gaps=FILL_GAPS,
            welfare_type="all",
            reporting_level="all",
            ppp_version=2017,
        )

        # Edit percentile file to get the list of different countries
        df_percentiles_pub = df_percentiles_published_2017.copy()
        df_percentiles_pub = df_percentiles_pub.drop(
            columns=["percentile", "avg_welfare", "pop_share", "welfare_share", "quantile"]
        ).drop_duplicates()

        # Merge the two files
        df_merge = pd.merge(
            df_reference,
            df_percentiles_pub,
            on=["country_code", "year", "reporting_level", "welfare_type"],
            how="outer",
            indicator=True,
        )

        # Obtain the list of countries that are in the reference file but not in the percentile file
        list_missing_countries = df_merge.loc[df_merge["_merge"] == "left_only", "country_code"].unique().tolist()

        # Generate a string with all the elements of the list, in the format for querying multiple countries in the API
        missing_countries = "&country=".join(list_missing_countries)

        return missing_countries, list_missing_countries

    # Obtain latest versions of the PIP dataset
    versions = pip_versions(wb_api)

    # Run the main function
    missing_countries, list_missing_countries = get_list_of_missing_countries()
    log.info(
        f"These countries are available in a common query but not in the percentile file: {list_missing_countries}"
    )

    # Only run the function if there are missing countries
    if list_missing_countries:
        concurrent_percentiles_function(country_code=missing_countries)
        log.info("Country files downloaded")
    else:
        log.info("All countries are in the percentile file. No need to extract them.")

    concurrent_percentiles_region_function()
    log.info("Region files downloaded")

    log.info("Now we are concatenating the files")

    if list_missing_countries:
        with ThreadPool(MAX_WORKERS) as pool:
            tasks = [
                (povline, ppp_version, missing_countries)
                for ppp_version in PPP_VERSIONS
                for povline in POV_LINES_COUNTRIES
            ]
            dfs = pool.starmap(get_query_country, tasks)

        df_country = pd.concat(dfs, ignore_index=True)
        log.info("Country files concatenated")

    with ThreadPool(MAX_WORKERS) as pool:
        tasks = [(povline, ppp_version) for ppp_version in PPP_VERSIONS for povline in POV_LINES_REGIONS]
        dfs = pool.starmap(get_query_region, tasks)

    df_region = pd.concat(dfs, ignore_index=True)
    log.info("Region files concatenated")

    # Create poverty_line_cents column, multiplying by 100, rounding and making it an integer
    if list_missing_countries:
        df_country["poverty_line_cents"] = round(df_country["poverty_line"] * 100).astype(int)

    df_region["poverty_line_cents"] = round(df_region["poverty_line"] * 100).astype(int)

    log.info("Checking if all the poverty lines are in the concatenated files")

    # Check if all the poverty lines are in the df in country and region df
    if list_missing_countries:
        assert set(df_country["poverty_line_cents"].unique()) == set(POV_LINES_COUNTRIES), log.fatal(
            "Not all poverty lines are in the country file!"
        )
    assert set(df_region["poverty_line_cents"].unique()) == set(POV_LINES_REGIONS), log.fatal(
        "Not all poverty lines are in the region file!"
    )

    # Drop poverty_line_cents column
    if list_missing_countries:
        df_country = df_country.drop(columns=["poverty_line_cents"])
    df_region = df_region.drop(columns=["poverty_line_cents"])

    log.info("Checking if the set of countries and regions is the same as in PIP")

    # I check if the set of countries is the same in the df and in the list of missing countries
    if list_missing_countries:
        assert set(df_country["country_code"].unique()) == set(list_missing_countries), log.fatal(
            f"List of countries is different from the one we needed to extract! ({list_missing_countries})"
        )

    # I check if the set of regions is the same in the df and in the aux table (list of regions)
    aux_dict = pip_aux_tables(wb_api, table="regions")
    assert set(df_region["country"].unique()) == set(aux_dict["regions"]["region"].unique()), log.fatal(
        "List of regions is not the same as the one defined in PIP!"
    )

    log.info("Concatenating the raw percentile data for countries and regions")

    # Concatenate df_country and df_region
    if list_missing_countries:
        df = pd.concat([df_country, df_region], ignore_index=True)
    else:
        df = df_region.copy()

    end_time = time.time()
    elapsed_time = round(end_time - start_time, 2)
    log.info(
        f"Concatenation of raw percentile data for countries and regions completed. Execution time: {elapsed_time} seconds"
    )

    return df


def calculate_percentile(p, df):
    """
    Calculates a single percentile and returns a DataFrame with the results.
    """
    # If reporting_level and welfare_type columns do not exist, add them with NaNs
    # This happens when only regional data is processed (missing countries are empty)
    if "reporting_level" not in df.columns:
        df["reporting_level"] = None
    if "welfare_type" not in df.columns:
        df["welfare_type"] = None

    df["distance_to_p"] = abs(df["headcount"] * 100 - p)
    df_closest = (
        df.sort_values("distance_to_p")
        .groupby(
            ["ppp_version", "country", "year", "reporting_level", "welfare_type"],
            as_index=False,
            sort=False,
            dropna=False,  # This is to avoid dropping rows with NaNs (reporting_level and welfare_type for regions)
        )
        .first()
    )
    df_closest["target_percentile"] = p
    df_closest = df_closest[
        [
            "ppp_version",
            "country",
            "year",
            "reporting_level",
            "welfare_type",
            "target_percentile",
            "poverty_line",
            "headcount",
            "distance_to_p",
        ]
    ]
    log.info(f"Percentile {p}: calculated")
    return df_closest


def format_official_percentiles(year, wb_api: WB_API):
    """
    Download percentiles from the World Bank Databank and format them to the same format as the constructed percentiles
    """
    # Load percentile files from the World Bank Databank
    df_percentiles_published = _fetch_percentiles(year)

    # Obtain country names from the aux table
    aux_dict = pip_aux_tables(wb_api, table="countries")
    df_countries = aux_dict["countries"]

    # Merge the two files to get country names
    df_percentiles_published = pd.merge(
        df_percentiles_published,
        df_countries[["country_code", "country_name"]],
        on="country_code",
        how="left",
    )

    # Rename columns
    df_percentiles_published = df_percentiles_published.rename(
        columns={
            "country_name": "country",
            "percentile": "target_percentile",
            "avg_welfare": "avg",
            "welfare_share": "share",
            "quantile": "thr",
        }
    )

    # Drop pop_share
    df_percentiles_published = df_percentiles_published.drop(columns=["pop_share"])

    # Make thr null if target_percentile is 100
    df_percentiles_published.loc[df_percentiles_published["target_percentile"] == 100, "thr"] = np.nan

    # Add ppp_version column
    df_percentiles_published["ppp_version"] = year

    return df_percentiles_published


def generate_consolidated_percentiles(df, wb_api: WB_API):
    """
    Generates percentiles from the raw data. This is the final file with percentiles.
    """
    start_time = time.time()

    path_file_percentiles = f"{CACHE_DIR}/pip_percentiles_before_checks.csv"

    if Path(path_file_percentiles).is_file():
        log.info("Percentiles file already exists. No need to consolidate.")
        df_percentiles = pd.read_csv(path_file_percentiles)

    else:
        log.info("Consolidating percentiles")

        # Define percentiles, from 1 to 99
        percentiles = range(1, 100, 1)
        df_percentiles = pd.DataFrame()

        # Estimate percentiles
        dfs = [calculate_percentile(p, df) for p in percentiles]

        df_percentiles = pd.concat(dfs, ignore_index=True)

        log.info("Percentiles calculated and consolidated")

        # Rename headcount to estimated_percentile and poverty_line to thr
        df_percentiles = df_percentiles.rename(columns={"headcount": "estimated_percentile", "poverty_line": "thr"})  # type: ignore

        # Add official percentiles from the World Bank Databank
        df_percentiles_published_2011 = format_official_percentiles(2011, wb_api)
        df_percentiles_published_2017 = format_official_percentiles(2017, wb_api)

        df_percentiles = pd.concat(
            [df_percentiles, df_percentiles_published_2011, df_percentiles_published_2017], ignore_index=True
        )

        # Drop duplicates. Keep the second one (the official one)
        df_percentiles = df_percentiles.drop_duplicates(
            subset=["ppp_version", "country", "year", "reporting_level", "welfare_type", "target_percentile"],
            keep="last",
        )

        # Sort by ppp_version, country, year, reporting_level, welfare_type and target_percentile
        df_percentiles = df_percentiles.sort_values(
            by=["ppp_version", "country", "year", "reporting_level", "welfare_type", "target_percentile"]
        )

        # Save to csv
        df_percentiles.to_csv(f"{CACHE_DIR}/pip_percentiles_before_checks.csv", index=False)

    # SANITY CHECKS
    df_percentiles = sanity_checks(df_percentiles)

    # Drop distance_to_p, estimated_percentile, country_code
    df_percentiles = df_percentiles.drop(columns=["distance_to_p", "estimated_percentile", "country_code"])

    # Rename target_percentile to percentile
    df_percentiles = df_percentiles.rename(columns={"target_percentile": "percentile"})

    # Save to csv
    df_percentiles.to_csv(f"{CACHE_DIR}/pip_percentiles.csv", index=False)

    end_time = time.time()
    elapsed_time = round(end_time - start_time, 2)
    log.info(f"Percentiles calculated and checked. Execution time: {elapsed_time} seconds")

    return df_percentiles


def sanity_checks(df_percentiles):
    """
    Run different sanity checks to the percentiles file.
    """
    log.info("Starting sanity checks")

    # Count number of rows before checks
    rows_before = len(df_percentiles)

    # Consecutive percentiles (1, 2, 3, etc)
    # Create a column called check that is True if target_percentile is consecutive for each ppp_version, country, year, reporting_level, and welfare_type
    df_percentiles["check"] = (
        df_percentiles.groupby(["ppp_version", "country", "year", "reporting_level", "welfare_type"], dropna=False)[
            "target_percentile"
        ].diff()
        == 1
    )

    # Replace check with True if target_percentile is 1
    df_percentiles.loc[df_percentiles["target_percentile"] == 1, "check"] = True

    # Assign the boolean value to the entire group
    df_percentiles["check"] = df_percentiles.groupby(
        ["ppp_version", "country", "year", "reporting_level", "welfare_type"], dropna=False
    )["check"].transform("all")

    # Define mask
    mask = ~df_percentiles["check"]
    df_error = df_percentiles[mask].reset_index(drop=True).copy()

    if len(df_error) > 0:
        log.warning(
            f"""Percentiles are not consecutive! These distributions will not be used:
                {df_error[["ppp_version", "country", "year", "reporting_level", "welfare_type"]].drop_duplicates()}"""
        )
        # Drop faulty distributions
        df_percentiles = df_percentiles[~mask].reset_index(drop=True)

    ############################################################################################################
    # Distance_to_p is higher than TOLERANCE_PERCENTILES
    df_percentiles["check"] = df_percentiles["distance_to_p"] > TOLERANCE_PERCENTILES

    # Assign the boolean value to the entire group
    df_percentiles["check"] = df_percentiles.groupby(
        ["ppp_version", "country", "year", "reporting_level", "welfare_type"], dropna=False
    )["check"].transform("any")

    # Define mask
    mask = df_percentiles["check"]
    df_error = df_percentiles[mask].reset_index(drop=True).copy()

    if len(df_error) > 0:
        log.warning(
            f"""Percentiles are not accurate! These distributions will not be used:
                {df_error[["ppp_version", "country", "year", "reporting_level", "welfare_type"]].drop_duplicates()}"""
        )
        # Drop faulty distributions
        df_percentiles = df_percentiles[~mask].reset_index(drop=True)

    ############################################################################################################
    # Nulls for thr, avg and share for the entire group of ppp_version, country, year, reporting_level, and welfare_type
    df_percentiles["check_thr"] = df_percentiles.groupby(
        ["ppp_version", "country", "year", "reporting_level", "welfare_type"], dropna=False
    )["thr"].transform(lambda x: x.isnull().all())
    df_percentiles["check_avg"] = df_percentiles.groupby(
        ["ppp_version", "country", "year", "reporting_level", "welfare_type"], dropna=False
    )["avg"].transform(lambda x: x.isnull().all())
    df_percentiles["check_share"] = df_percentiles.groupby(
        ["ppp_version", "country", "year", "reporting_level", "welfare_type"], dropna=False
    )["share"].transform(lambda x: x.isnull().all())

    df_percentiles["check"] = df_percentiles["check_thr"] & df_percentiles["check_avg"] & df_percentiles["check_share"]

    # Define mask
    mask = df_percentiles["check"]
    df_error = df_percentiles[mask].reset_index(drop=True).copy()

    if len(df_error) > 0:
        log.warning(
            f"""There are null values for thr, avg and share! These distributions need to be corrected:
                {df_error[["ppp_version", "country", "year", "reporting_level", "welfare_type"]].drop_duplicates()}"""
        )
        # Drop distributions with null values for thr, avg and share
        df_percentiles = df_percentiles[~mask].reset_index(drop=True)

    ############################################################################################################
    # Find negative values for thr
    df_percentiles["check"] = df_percentiles["thr"] < 0

    # Define mask
    mask = df_percentiles["check"]

    df_error = df_percentiles[mask].reset_index(drop=True).copy()

    if len(df_error) > 0:
        log.warning(
            f"""There are negative values for thr! These distributions need to be corrected:
                {df_error[["ppp_version", "country", "year", "reporting_level", "welfare_type"]].drop_duplicates()}"""
        )
        # Correct cases where thr, avg and share are negative, by assigning 0
        df_percentiles.loc[mask, "thr"] = 0

    ############################################################################################################
    # Find negative values for avg
    df_percentiles["check"] = df_percentiles["avg"] < 0

    # Define mask
    mask = df_percentiles["check"]

    df_error = df_percentiles[mask].reset_index(drop=True).copy()

    if len(df_error) > 0:
        log.warning(
            f"""There are negative values for avg! These distributions need to be corrected:
                {df_error[["ppp_version", "country", "year", "reporting_level", "welfare_type"]].drop_duplicates()}"""
        )
        # Correct cases where thr, avg and share are negative, by assigning 0
        df_percentiles.loc[mask, "avg"] = 0

    ############################################################################################################
    # Find negative values for share
    df_percentiles["check"] = df_percentiles["share"] < 0

    # Define mask
    mask = df_percentiles["check"]

    df_error = df_percentiles[mask].reset_index(drop=True).copy()

    if len(df_error) > 0:
        log.warning(
            f"""There are negative values for share! These distributions need to be corrected:
                {df_error[["ppp_version", "country", "year", "reporting_level", "welfare_type"]].drop_duplicates()}"""
        )
        # Correct cases where thr, avg and share are negative, by assigning 0
        df_percentiles.loc[mask, "share"] = 0

    ############################################################################################################
    # thr is increasing for each ppp_version, country, year, reporting_level, and welfare_type
    df_percentiles["check"] = (
        df_percentiles.groupby(["ppp_version", "country", "year", "reporting_level", "welfare_type"], dropna=False)[
            "thr"
        ]
        .diff()
        .round(2)
        >= 0
    )

    # Replace check with True if thr is NaN
    df_percentiles.loc[df_percentiles["thr"].isna(), "check"] = True

    # Replace check with True if target_percentile is 1
    df_percentiles.loc[(df_percentiles["target_percentile"] == 1), "check"] = True

    # Define mask
    mask = ~df_percentiles["check"]
    df_error = df_percentiles[mask].reset_index(drop=True).copy()

    if len(df_error) > 0:
        log.warning(
            f"""Thresholds are not increasing! These distributions need to be corrected:
                {df_error[["ppp_version", "country", "year", "reporting_level", "welfare_type"]].drop_duplicates()}"""
        )
        # Correct cases where thr is not increasing, by repeating the previous thr
        df_percentiles.loc[mask, "thr"] = df_percentiles.loc[mask, "thr"].shift(1)

    ############################################################################################################
    # avg is increasing for each ppp_version, country, year, reporting_level, and welfare_type
    df_percentiles["check"] = (
        df_percentiles.groupby(["ppp_version", "country", "year", "reporting_level", "welfare_type"], dropna=False)[
            "avg"
        ]
        .diff()
        .round(2)
        >= 0
    )

    # Replace check with True if avg is NaN
    df_percentiles.loc[df_percentiles["avg"].isna(), "check"] = True

    # Replace check with True if target_percentile is 1
    df_percentiles.loc[(df_percentiles["target_percentile"] == 1), "check"] = True

    # Define mask
    mask = ~df_percentiles["check"]
    df_error = df_percentiles[mask].reset_index(drop=True).copy()

    if len(df_error) > 0:
        log.warning(
            f"""Averages are not increasing! These distributions need to be corrected:
                {df_error[["ppp_version", "country", "year", "reporting_level", "welfare_type"]].drop_duplicates()}"""
        )
        # Correct cases where avg is not increasing, by repeating the previous avg
        df_percentiles.loc[mask, "avg"] = df_percentiles.loc[mask, "avg"].shift(1)

    ############################################################################################################
    # Check that avg are between thresholds
    # Create thr_lower, which is the threshold for the previous percentile
    df_percentiles["thr_lower"] = df_percentiles.groupby(
        ["ppp_version", "country", "year", "reporting_level", "welfare_type"], dropna=False
    )["thr"].shift(1)
    df_percentiles["check"] = (round(df_percentiles["avg"] - df_percentiles["thr_lower"], 2) >= 0) & (
        round(df_percentiles["thr"] - df_percentiles["avg"]) >= 0
    )

    # Assign True if target_percentile is 1
    df_percentiles.loc[df_percentiles["target_percentile"] == 1, "check"] = True

    # Assign True if target_percentile is 100 and avg is greater than thr_lower
    df_percentiles.loc[
        (df_percentiles["target_percentile"] == 100)
        & (round(df_percentiles["avg"] - df_percentiles["thr_lower"], 2) >= 0),
        "check",
    ] = True

    # Assign True if avg is null
    df_percentiles.loc[df_percentiles["avg"].isnull(), "check"] = True

    # Assign the boolean value to the entire group
    df_percentiles["check"] = df_percentiles.groupby(
        ["ppp_version", "country", "year", "reporting_level", "welfare_type"], dropna=False
    )["check"].transform("all")

    # Define mask
    mask = ~df_percentiles["check"]
    df_error = df_percentiles[mask].reset_index(drop=True).copy()

    if len(df_error) > 0:
        log.warning(
            f"""Averages are not between thresholds! These distributions need to be corrected:
                {df_error[["ppp_version", "country", "year", "reporting_level", "welfare_type"]].drop_duplicates()}"""
        )
        # Correct cases where avg is not between thresholds, by averaging the two thresholds
        df_percentiles.loc[mask, "avg"] = (df_percentiles.loc[mask, "thr_lower"] + df_percentiles.loc[mask, "thr"]) / 2

    # Drop check columns
    df_percentiles = df_percentiles.drop(columns=["check", "check_thr", "check_avg", "check_share", "thr_lower"])

    # Count number of rows after checks
    rows_after = len(df_percentiles)

    log.info(f"Percentiles file generated. {rows_before - rows_after} rows have been deleted.")

    return df_percentiles


# GENERATE RELATIVE POVERTY INDICATORS FILE
# This is data not given directly by the query, but we can get it by calculating 40, 50, 60% of the median and query
# NOTE: Medians need to be patched first in order to get data for all country-years (there are several missing values)


def generate_relative_poverty(wb_api: WB_API):
    """
    Generates relative poverty indicators from query results. Uses concurrent.futures to speed up the process.
    """
    start_time = time.time()

    def get_relative_data(df_row, pct, versions):
        """
        This function is structured in a way to make it work with concurrent.futures.
        It checks if the country file related to the row exists. If not, it runs the query.
        """
        if ~np.isnan(df_row["median"]):
            if Path(
                f"{CACHE_DIR}/pip_country_data/pip_country_{df_row['country_code']}_year_{df_row['year']}_povline_{int(round(df_row['median'] * pct))}_welfare_{df_row['welfare_type']}_rep_{df_row['reporting_level']}_fillgaps_{FILL_GAPS}_ppp_2017.csv"
            ).is_file():
                return
            else:
                return pip_query_country(
                    wb_api,
                    popshare_or_povline="povline",
                    value=df_row["median"] * pct / 100,
                    versions=versions,
                    country_code=df_row["country_code"],
                    year=df_row["year"],
                    fill_gaps=FILL_GAPS,
                    welfare_type=df_row["welfare_type"],
                    reporting_level=df_row["reporting_level"],
                    ppp_version=2017,
                    download="true",
                )

    def concurrent_relative_function(df):
        """
        This is the main function to make concurrency work for country data.
        """
        # Make sure the directory exists. If not, create it
        Path(f"{CACHE_DIR}/pip_country_data").mkdir(parents=True, exist_ok=True)
        with ThreadPool(MAX_WORKERS) as pool:
            tasks = [(df.iloc[i], pct, versions) for pct in [40, 50, 60] for i in range(len(df))]
            pool.starmap(get_relative_data, tasks)

    def get_relative_data_region(df_row, pct, versions):
        """
        This function is structured in a way to make it work with concurrent.futures.
        It checks if the regional file related to the row exists. If not, it runs the query.
        """
        if ~np.isnan(df_row["median"]):
            if Path(
                f"{CACHE_DIR}/pip_region_data/pip_region_{df_row['country_code']}_year_{df_row['year']}_povline_{int(round(df_row['median']*pct))}_ppp_2017.csv"
            ).is_file():
                return
            else:
                return pip_query_region(
                    wb_api,
                    popshare_or_povline="povline",
                    value=df_row["median"] * pct / 100,
                    versions=versions,
                    country_code=df_row["country_code"],
                    year=df_row["year"],
                    welfare_type="all",
                    reporting_level="all",
                    ppp_version=2017,
                    download="true",
                )

    def concurrent_relative_region_function(df):
        """
        This is the main function to make concurrency work for regional data.
        """
        # Make sure the directory exists. If not, create it
        Path(f"{CACHE_DIR}/pip_region_data").mkdir(parents=True, exist_ok=True)
        with ThreadPool(int(round(MAX_WORKERS / 2))) as pool:
            tasks = [(df.iloc[i], pct, versions) for pct in [40, 50, 60] for i in range(len(df))]
            pool.starmap(get_relative_data_region, tasks)

    def add_relative_indicators(df, country_or_region):
        """
        Integrates the relative indicators to the df.
        """
        for pct in [40, 50, 60]:
            # Initialize lists
            headcount_ratio_list = []
            pgi_list = []
            pov_severity_list = []
            watts_list = []
            for i in range(len(df)):
                if ~np.isnan(df["median"].iloc[i]):
                    if country_or_region == "country":
                        # Here I check if the file exists even after the original extraction. If it does, I read it. If not, I start the queries again.
                        file_path = f"{CACHE_DIR}/pip_country_data/pip_country_{df.iloc[i]['country_code']}_year_{df.iloc[i]['year']}_povline_{int(round(df.iloc[i]['median']*pct))}_welfare_{df.iloc[i]['welfare_type']}_rep_{df.iloc[i]['reporting_level']}_fillgaps_{FILL_GAPS}_ppp_2017.csv"
                        if Path(file_path).is_file():
                            results = pd.read_csv(file_path)
                        else:
                            # Run the main function to get the data
                            get_relative_data(df.iloc[i], pct, versions)
                            results = pd.read_csv(file_path)

                    elif country_or_region == "region":
                        # Here I check if the file exists even after the original extraction. If it does, I read it. If not, I start the queries again.
                        file_path = f"{CACHE_DIR}/pip_region_data/pip_region_{df.iloc[i]['country_code']}_year_{df.iloc[i]['year']}_povline_{int(round(df.iloc[i]['median']*pct))}_ppp_2017.csv"
                        if Path(file_path).is_file():
                            results = pd.read_csv(file_path)
                        else:
                            # Run the main function to get the data
                            get_relative_data_region(df.iloc[i], pct, versions)
                            results = pd.read_csv(file_path)
                    else:
                        raise ValueError("country_or_region must be 'country' or 'region'")

                    headcount_ratio_value = results["headcount"].iloc[0]
                    headcount_ratio_list.append(headcount_ratio_value)

                    pgi_value = results["poverty_gap"].iloc[0]
                    pgi_list.append(pgi_value)

                    pov_severity_value = results["poverty_severity"].iloc[0]
                    pov_severity_list.append(pov_severity_value)

                    watts_value = results["watts"].iloc[0]
                    watts_list.append(watts_value)

                else:
                    headcount_ratio_list.append(np.nan)
                    pgi_list.append(np.nan)
                    pov_severity_list.append(np.nan)
                    watts_list.append(np.nan)

            # Add the lists as columns to the df
            df[f"headcount_ratio_{pct}_median"] = headcount_ratio_list
            df[f"poverty_gap_index_{pct}_median"] = pgi_list
            df[f"poverty_severity_{pct}_median"] = pov_severity_list
            df[f"watts_{pct}_median"] = watts_list

        return df

    # Obtain versions
    versions = pip_versions(wb_api)

    # FOR COUNTRIES
    # Get data from the most common query
    df_country = pip_query_country(
        wb_api,
        popshare_or_povline="povline",
        value=2.15,
        versions=versions,
        country_code="all",
        year="all",
        fill_gaps=FILL_GAPS,
        welfare_type="all",
        reporting_level="all",
        ppp_version=2017,
    )

    # Patch medians
    df_country = median_patch(df_country, country_or_region="country")

    # Run the main function to get the data
    concurrent_relative_function(df_country)

    # Add relative indicators from the results above
    df_country = add_relative_indicators(df=df_country, country_or_region="country")

    # FOR REGIONS
    # Get data from the most common query
    df_region = pip_query_region(
        wb_api,
        popshare_or_povline="povline",
        value=2.15,
        versions=versions,
        country_code="all",
        year="all",
        welfare_type="all",
        reporting_level="all",
        ppp_version=2017,
    )

    # Patch medians
    df_region = median_patch(df_region, country_or_region="region")

    # Run the main function to get the data
    concurrent_relative_region_function(df_region)

    # Add relative indicators from the results above
    df_region = add_relative_indicators(df=df_region, country_or_region="region")

    # Concatenate df_country and df_region
    df = pd.concat([df_country, df_region], ignore_index=True)

    # Save to csv
    df.to_csv(f"{CACHE_DIR}/pip_relative.csv", index=False)

    end_time = time.time()
    elapsed_time = round(end_time - start_time, 2)
    log.info(f"Relative poverty indicators calculated. Execution time: {elapsed_time} seconds")

    return df


# GENERATE MAIN INDICATORS FILE


def generate_key_indicators(wb_api: WB_API):
    """
    Generate the main indicators file, from a set of poverty lines and PPP versions. Uses concurrent.futures to speed up the process.
    """
    start_time = time.time()

    def get_country_data(povline, ppp_version, versions):
        """
        This function is defined inside the main function because it needs to be called by concurrent.futures.
        For country data.
        """
        return pip_query_country(
            wb_api,
            popshare_or_povline="povline",
            value=povline / 100,
            versions=versions,
            country_code="all",
            year="all",
            fill_gaps=FILL_GAPS,
            welfare_type="all",
            reporting_level="all",
            ppp_version=ppp_version,
            download="false",
        )

    def get_region_data(povline, ppp_version, versions):
        """
        This function is defined inside the main function because it needs to be called by concurrent.futures.
        For regional data.
        """
        return pip_query_region(
            wb_api,
            popshare_or_povline="povline",
            value=povline / 100,
            versions=versions,
            country_code="all",
            year="all",
            welfare_type="all",
            reporting_level="all",
            ppp_version=ppp_version,
            download="false",
        )

    def concurrent_function():
        """
        This function makes concurrency work for country data.
        """
        with ThreadPool(MAX_WORKERS) as pool:
            tasks = [
                (povline, ppp_version, versions)
                for ppp_version, povlines in POVLINES_DICT.items()
                for povline in povlines
            ]
            results = pool.starmap(get_country_data, tasks)

        # Concatenate list of dataframes
        results = pd.concat(results, ignore_index=True)

        return results

    def concurrent_region_function():
        """
        This function makes concurrency work for regional data.
        """
        with ThreadPool(int(round(MAX_WORKERS / 2))) as pool:
            tasks = [
                (povline, ppp_version, versions)
                for ppp_version, povlines in POVLINES_DICT.items()
                for povline in povlines
            ]
            results = pool.starmap(get_region_data, tasks)

        # Concatenate list of dataframes
        results = pd.concat(results, ignore_index=True)

        return results

    def get_china_india_data_filled(povline, ppp_version, versions):
        """
        This function extracts filled data for China and India to be used in the key indicators file.
        """
        return pip_query_country(
            wb_api,
            popshare_or_povline="povline",
            value=povline / 100,
            versions=versions,
            country_code="CHN&country=IND",
            year="all",
            fill_gaps="true",
            welfare_type="all",
            reporting_level="national",
            ppp_version=ppp_version,
            download="false",
        )

    def concurrent_function_china_india():
        """
        This function makes concurrency work for China and India data.
        """
        with ThreadPool(MAX_WORKERS) as pool:
            tasks = [
                (povline, ppp_version, versions)
                for ppp_version, povlines in POVLINES_DICT.items()
                for povline in povlines
            ]
            results = pool.starmap(get_china_india_data_filled, tasks)

        # Concatenate list of dataframes
        results = pd.concat(results, ignore_index=True)

        return results

    # Obtain latest versions of the PIP dataset
    versions = pip_versions(wb_api)

    # Run the main function
    results = concurrent_function()
    results_region = concurrent_region_function()

    # Query China and India data
    results_china_india = concurrent_function_china_india()

    # Calculate World (excluding China) and World (excluding India) data
    results_region = calculate_world_excluding_china_and_india(results_region, results_china_india)

    # If country is nan but country_code is TWN, replace country with Taiwan, China
    results.loc[results["country"].isnull() & (results["country_code"] == "TWN"), "country"] = "Taiwan, China"

    # I check if the set of countries is the same in the df and in the aux table (list of countries)
    aux_dict = pip_aux_tables(wb_api, table="countries")
    assert set(results["country"]) == set(aux_dict["countries"]["country_name"]), log.fatal(
        f"List of countries is not the same! Differences: {set(results['country']) - set(aux_dict['countries']['country_name'])}"
    )

    # # I check if the set of regions is the same in the df and in the aux table (list of regions) + World (excluding China) + World (excluding India)
    # aux_dict = pip_aux_tables(wb_api, table="regions")

    # countries_to_check = set(aux_dict["regions"]["region"]) | {"World (excluding China)", "World (excluding India)"}

    # assert set(results_region["country"]) == (countries_to_check), log.fatal(
    #     f"List of regions is not the same! Differences: {set(results_region['country']) - countries_to_check}"
    # )

    # Concatenate df_country and df_region
    df = pd.concat([results, results_region], ignore_index=True)

    # Sort ppp_version, country, year and poverty_line
    df = df.sort_values(by=["ppp_version", "country", "year", "poverty_line"])  # type: ignore

    # Save to csv
    df.to_csv(f"{CACHE_DIR}/pip_raw.csv", index=False)

    end_time = time.time()
    elapsed_time = round(end_time - start_time, 2)
    log.info(f"Key indicators calculated. Execution time: {elapsed_time} seconds")

    return df


def calculate_world_excluding_china_and_india(results_region: pd.DataFrame, results_china_india: pd.DataFrame):
    """
    Calculate World (excluding China) and World (excluding India) data.
    """

    results_region = results_region.copy()
    results_china_india = results_china_india.copy()

    # Filter results to show only World
    results_world = results_region[results_region["country"] == "World"].copy().reset_index(drop=True)

    # Keep country, year, poverty_line and headcount columns
    results_world = results_world[["ppp_version", "country", "year", "poverty_line", "headcount", "reporting_pop"]]
    results_china_india = results_china_india[
        ["ppp_version", "country", "year", "poverty_line", "headcount", "reporting_pop"]
    ]

    # Create headcount_ratio column
    results_world["headcount_number"] = results_world["headcount"] * results_world["reporting_pop"]
    results_china_india["headcount_number"] = results_china_india["headcount"] * results_china_india["reporting_pop"]

    # Make these columns integer
    results_world["headcount_number"] = results_world["headcount_number"].astype(int)
    results_china_india["headcount_number"] = results_china_india["headcount_number"].astype(int)

    # Merge results_world and results_china_india
    results_excluding = pd.merge(
        results_china_india,
        results_world,
        on=["ppp_version", "year", "poverty_line"],
        how="left",
        suffixes=("", "_world"),
    )

    # Calculate headcount_excluding as the difference between headcount_world and headcount
    results_excluding["headcount_number_excluding"] = (
        results_excluding["headcount_number_world"] - results_excluding["headcount_number"]
    )

    # Same with reporting_pop
    results_excluding["reporting_pop_excluding"] = (
        results_excluding["reporting_pop_world"] - results_excluding["reporting_pop"]
    )

    # Estimate headcount_excluding
    results_excluding["headcount_excluding"] = (
        results_excluding["headcount_number_excluding"] / results_excluding["reporting_pop_excluding"]
    )

    # Keep country, year , poverty_line, headcount_excluding and reporting_pop_excluding columns
    results_excluding = results_excluding[
        ["ppp_version", "country", "year", "poverty_line", "headcount_excluding", "reporting_pop_excluding"]
    ]

    # Rename countries to World (excluding China) and World (excluding India)
    results_excluding["country"] = results_excluding["country"].replace(
        {"China": "World (excluding China)", "India": "World (excluding India)"}
    )

    # Rename columns to headcount and reporting_pop
    results_excluding = results_excluding.rename(
        columns={"headcount_excluding": "headcount", "reporting_pop_excluding": "reporting_pop"}
    )

    # Concatenate tables
    results_region = pd.concat([results_region, results_excluding], ignore_index=True)

    return results_region


def median_patch(df, country_or_region):
    """
    Patch missing values in the median column.
    PIP queries do not return all the medians, so they are patched with the results of the percentile file.
    """

    # Read percentile file
    df_percentiles = pd.read_csv(f"{CACHE_DIR}/pip_percentiles.csv")

    # In df_percentiles, keep only the rows with percentile = 50
    df_percentiles = df_percentiles[df_percentiles["percentile"] == 50].reset_index()

    # If I want to patch the median for regions, I need to drop reporting_level and welfare_type columns
    if country_or_region == "country":
        # Merge df and df_percentiles
        df = pd.merge(
            df,
            df_percentiles[["ppp_version", "country", "year", "reporting_level", "welfare_type", "thr"]],
            on=["ppp_version", "country", "year", "reporting_level", "welfare_type"],
            how="left",
        )

        # Replace missing values in median with thr
        df["median"] = df["median"].fillna(df["thr"])

        # Drop thr column
        df = df.drop(columns=["thr"])

    elif country_or_region == "region":
        # Merge df and df_percentiles
        df = pd.merge(
            df,
            df_percentiles[["ppp_version", "country", "year", "thr"]],
            on=["ppp_version", "country", "year"],
            how="left",
        )

        # Rename thr to median
        df = df.rename(columns={"thr": "median"})

    else:
        raise ValueError("country_or_region must be 'country' or 'region'")

    log.info("Medians patched!")

    return df


def add_relative_poverty_and_decile_thresholds(df, df_relative, df_percentiles, wb_api: WB_API):
    """
    Add relative poverty indicators and decile thresholds to the key indicators file.
    """

    # Add relative poverty indicators
    # They don't change with the PPP version, so we can use the 2017 version I estimated before.
    df = pd.merge(
        df,
        df_relative[
            [
                "country",
                "year",
                "reporting_level",
                "welfare_type",
                "headcount_ratio_40_median",
                "poverty_gap_index_40_median",
                "poverty_severity_40_median",
                "watts_40_median",
                "headcount_ratio_50_median",
                "poverty_gap_index_50_median",
                "poverty_severity_50_median",
                "watts_50_median",
                "headcount_ratio_60_median",
                "poverty_gap_index_60_median",
                "poverty_severity_60_median",
                "watts_60_median",
            ]
        ],
        on=["country", "year", "reporting_level", "welfare_type"],
        how="left",
    )

    # In df_percentiles, keep only the rows with percentile = 10, 20, 30, ... 90
    df_percentiles = df_percentiles[
        (df_percentiles["percentile"] % 10 == 0) & (df_percentiles["percentile"] != 100)
    ].reset_index()

    # Make tb_percentile wide, with percentile as columns
    df_percentiles = df_percentiles.pivot(
        index=["ppp_version", "country", "year", "reporting_level", "welfare_type"],
        columns="percentile",
        values="thr",
    )

    # Rename columns
    df_percentiles.columns = ["decile" + str(int(round(col / 10))) + "_thr" for col in df_percentiles.columns]

    # Reset index
    df_percentiles = df_percentiles.reset_index()

    # Merge df and df_percentiles
    df = pd.merge(
        df,
        df_percentiles,
        on=["ppp_version", "country", "year", "reporting_level", "welfare_type"],
        how="left",
    )

    # Add regional definitions
    df = add_regional_definitions(wb_api, df=df)

    # Save key indicators file
    df.to_csv(f"{CACHE_DIR}/world_bank_pip.csv", index=False)

    log.info("Relative poverty indicators and decile thresholds added. Key indicators file done :)")

    return df


def add_regional_definitions(wb_api: WB_API, df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract the complete definitions of regions and their countries from the World Bank API.
    This is a more complete version of the regional definitions that are already in the PIP dataset (Saudi Arabia, for example, is missing).
    """

    # Remove region_name column
    df = df.drop(columns=["region_name"])

    # Get regional definitions
    df_regional_definitions = pip_aux_tables(wb_api, table="country_list")

    # Make it a dataframe
    df_regional_definitions = pd.DataFrame.from_dict(df_regional_definitions["country_list"])

    # Rename country_name to country
    df_regional_definitions = df_regional_definitions.rename(
        columns={"country_name": "country", "region": "region_name"}
    )

    # Keep the relevant columns
    df_regional_definitions = df_regional_definitions[["country", "region_name"]]

    # Define MAX_YEAR as the maximum year in the df
    MAX_YEAR = df["year"].max()

    # Add year = MAX_YEAR to the regional definitions
    df_regional_definitions["year"] = MAX_YEAR

    # For each year in PPP_VERSIONS, add ppp_version
    df_regional_definitions_both_ppps = []
    for ppp_year in PPP_VERSIONS:
        df_regional_definitions["ppp_version"] = ppp_year

        df_regional_definitions_both_ppps.append(df_regional_definitions)

    df_regional_definitions_both_ppps = pd.concat(df_regional_definitions_both_ppps, ignore_index=True)

    # Merge df with regional definitions
    df = pd.merge(df, df_regional_definitions, on=["country", "year", "ppp_version"], how="outer")

    return df


if __name__ == "__main__":
    run()
