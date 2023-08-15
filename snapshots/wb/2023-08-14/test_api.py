import io
from pathlib import Path

import pandas as pd
import requests
from structlog import get_logger

# Initialize logger.
log = get_logger()

PARENT_DIR = Path(__file__).parent.absolute()
MAX_REPEATS = 10
TIMEOUT = 500


def api_health():
    """
    Check if the API is running and download aux tables if it is.
    """
    # Initialize repeat counter
    repeat = 0

    # health comes from a json containing the status
    health = pd.read_json("https://api.worldbank.org/pip/v1/health-check")[0][0]

    # If the status is different to "PIP API is running", repeat the request until MAX_REPEATS
    while health != "PIP API is running" and repeat < MAX_REPEATS:
        repeat += 1

    # If the status is different to "PIP API is running" after MAX_REPEATS, log fatal error
    assert repeat < MAX_REPEATS, log.fatal(f"Health check: {health} (repeated {repeat} times)")


def pip_aux_tables(table="all") -> pd.DataFrame:
    """
    Download aux tables if the API is running.
    """

    api_health()

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
        for table in aux_tables_list:
            df = pd.read_csv(f"https://api.worldbank.org/pip/v1/aux?table={table}&long_format=false&format=csv")

            # Add table to df_dict
            df_dict[table] = df

    else:
        df = pd.read_csv(f"https://api.worldbank.org/pip/v1/aux?table={table}&long_format=false&format=csv")

        # Add table to df_dict
        df_dict = {table: df}

    log.info(f'Auxiliary tables downloaded ("{table}")')

    return df_dict


def pip_versions() -> dict:
    """
    Download aux tables if the API is running.
    """

    api_health()

    df = pd.read_csv("https://api.worldbank.org/pip/v1/versions?format=csv")
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
    popshare_or_povline,
    value,
    versions,
    country_code="all",
    year="all",
    fill_gaps="true",
    welfare_type="all",
    reporting_level="all",
    ppp_version=2017,
) -> pd.DataFrame:
    """
    Query the PIP API.
    """

    api_health()

    version = versions[ppp_version]["version"]
    release_version = versions[ppp_version]["release_version"]

    # Build query
    request_url = f"https://api.worldbank.org/pip/v1/pip?{popshare_or_povline}={value}&country={country_code}&year={year}&fill_gaps={fill_gaps}&welfare_type={welfare_type}&reporting_level={reporting_level}&ppp_version={ppp_version}&version={version}&release_version={release_version}&format=csv"
    status = 0
    repeat = 0

    # Repeat request until status is 200 or until MAX_REPEATS
    while status != 200 and repeat < MAX_REPEATS:
        response = requests.get(request_url, timeout=TIMEOUT)
        content = response.content
        status = response.status_code
        repeat += 1

    # After MAX_REPEATS, log fatal error
    assert repeat < MAX_REPEATS, log.fatal(f"Repeated {repeat} times, can't extract data")

    df = pd.read_csv(io.StringIO(content.decode("utf-8")))

    # Add PPP version as column
    df["ppp_version"] = ppp_version

    # Replace names of columns and drop redundancies
    df = df.rename(columns={"country_name": "country", "reporting_year": "year"})
    df = df.drop(columns=["region_name", "region_code", "country_code"])

    # Reorder columns: ppp_version, country, year, povline and the rest
    first_columns = ["ppp_version", "country", "year", "poverty_line"]
    df = df[first_columns + [column for column in df.columns if column not in first_columns]]

    log.info(f"Country data extracted for {popshare_or_povline} = {value} ({ppp_version} PPPs)")

    return df


def pip_query_region(
    popshare_or_povline,
    value,
    versions,
    country_code="all",
    year="all",
    welfare_type="all",
    reporting_level="all",
    ppp_version=2017,
) -> pd.DataFrame:
    """
    Query the PIP API.
    """

    api_health()

    version = versions[ppp_version]["version"]
    release_version = versions[ppp_version]["release_version"]

    # Build query
    request_url = f"https://api.worldbank.org/pip/v1/pip-grp?{popshare_or_povline}={value}&country={country_code}&year={year}&welfare_type={welfare_type}&reporting_level={reporting_level}&ppp_version={ppp_version}&version={version}&release_version={release_version}&format=csv"
    status = 0
    repeat = 0

    # Repeat request until status is 200 or until MAX_REPEATS
    while status != 200 and repeat < MAX_REPEATS:
        response = requests.get(request_url, timeout=TIMEOUT)
        content = response.content
        status = response.status_code
        repeat += 1

    # After MAX_REPEATS, log fatal error
    assert repeat < MAX_REPEATS, log.fatal(f"Repeated {repeat} times, can't extract data")

    df = pd.read_csv(io.StringIO(content.decode("utf-8")))

    # Add PPP version as column
    df["ppp_version"] = ppp_version

    # Replace names of columns and drop redundancies
    df = df.rename(columns={"region_name": "country", "reporting_year": "year"})
    df = df.drop(columns=["region_code"])

    # Reorder columns: ppp_version, country, year, povline and the rest
    first_columns = ["ppp_version", "country", "year", "poverty_line"]
    df = df[first_columns + [column for column in df.columns if column not in first_columns]]

    log.info(f"Regional data extracted for {popshare_or_povline} = {value} ({ppp_version} PPPs)")

    return df


#############################################
#                                           #
#               MAIN FUNCTION               #
#                                           #
#############################################

povlines_dict = {
    2011: [100, 190, 320, 550, 1000, 2000, 3000, 4000],
    2017: [100, 215, 365, 685, 1000, 2000, 3000, 4000],
}

versions = pip_versions()

df_country = pd.DataFrame()
df_region = pd.DataFrame()
for ppp_version, povlines in povlines_dict.items():
    for povline in povlines:
        df_query = pip_query_country(
            popshare_or_povline="povline",
            value=povline / 100,
            versions=versions,
            country_code="all",
            year="all",
            fill_gaps="false",
            welfare_type="all",
            reporting_level="all",
            ppp_version=ppp_version,
        )
        df_country = pd.concat([df_country, df_query], ignore_index=True)

# I check if the set of countries is the same in the df and in the aux table (list of countries)
aux_dict = pip_aux_tables(table="countries")
assert set(df_country["country"].unique()) == set(aux_dict["countries"]["country_name"].unique()), log.fatal(
    "List of countries is not the same!"
)

for ppp_version, povlines in povlines_dict.items():
    for povline in povlines:
        df_query = pip_query_region(
            popshare_or_povline="povline",
            value=povline / 100,
            versions=versions,
            country_code="all",
            year="all",
            welfare_type="all",
            reporting_level="all",
            ppp_version=ppp_version,
        )
        df_region = pd.concat([df_region, df_query], ignore_index=True)

# I check if the set of regions is the same in the df and in the aux table (list of regions)
aux_dict = pip_aux_tables(table="regions")
assert set(df_region["country"].unique()) == set(aux_dict["regions"]["region"].unique()), log.fatal(
    "List of regions is not the same!"
)

# Concatenate df_country and df_region
df = pd.concat([df_country, df_region], ignore_index=True)

# Sort ppp_version, country, year and poverty_line
df = df.sort_values(by=["ppp_version", "country", "year", "poverty_line"])

# Save to csv
df.to_csv(f"{PARENT_DIR}/pip.csv", index=False)
