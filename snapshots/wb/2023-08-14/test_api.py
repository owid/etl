import concurrent.futures
import io
import time
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from structlog import get_logger

# Initialize logger.
log = get_logger()

# Basic parameters to use in the functions
PARENT_DIR = Path(__file__).parent.absolute()
MAX_REPEATS = 10
TIMEOUT = 500
FILL_GAPS = "true"
MAX_WORKERS = 16


def api_health():
    """
    Check if the API is running.
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
        for tab in aux_tables_list:
            df = pd.read_csv(f"https://api.worldbank.org/pip/v1/aux?table={tab}&long_format=false&format=csv")

            # Add table to df_dict
            df_dict[tab] = df

    else:
        df = pd.read_csv(f"https://api.worldbank.org/pip/v1/aux?table={table}&long_format=false&format=csv")

        # Add table to df_dict
        df_dict = {table: df}

    log.info(f'Auxiliary tables downloaded ("{table}")')

    return df_dict


def pip_versions() -> dict:
    """
    Download latest PIP data versions if the API is running.
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
    download="false",
) -> pd.DataFrame:
    """
    Query country data from the PIP API.
    """

    api_health()

    value = round(value, 2)

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
    df = df.drop(columns=["region_name", "region_code"])

    # Reorder columns: ppp_version, country, year, povline and the rest
    first_columns = ["ppp_version", "country", "year", "poverty_line"]
    df = df[first_columns + [column for column in df.columns if column not in first_columns]]

    if download == "true":
        # make sure the directory exists. If not, create it
        Path(f"{PARENT_DIR}/pip_country_data").mkdir(parents=True, exist_ok=True)
        # Save to csv
        df.to_csv(
            f"{PARENT_DIR}/pip_country_data/pip_country_{country_code}_year_{year}_{popshare_or_povline}_{int(value*100)}_welfare_{welfare_type}_rep_{reporting_level}_fillgaps_{fill_gaps}.csv",
            index=False,
        )

    if country_code == "all":
        log.info(f"Country data extracted for {popshare_or_povline} = {value} ({ppp_version} PPPs)")
    else:
        log.info(
            f"Country data extracted for {popshare_or_povline} = {value} ({ppp_version} PPPs) in {country_code} {year}"
        )

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
    download="false",
) -> pd.DataFrame:
    """
    Query regional data from the PIP API.
    """

    api_health()

    value = round(value, 2)

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
    df = df.rename(columns={"region_name": "country", "reporting_year": "year", "region_code": "country_code"})

    # Reorder columns: ppp_version, country, year, povline and the rest
    first_columns = ["ppp_version", "country", "year", "poverty_line"]
    df = df[first_columns + [column for column in df.columns if column not in first_columns]]

    if download == "true":
        # make sure the directory exists. If not, create it
        Path(f"{PARENT_DIR}/pip_region_data").mkdir(parents=True, exist_ok=True)
        # Save to csv
        df.to_csv(
            f"{PARENT_DIR}/pip_region_data/pip_country_{country_code}_year_{year}_{popshare_or_povline}_{int(value*100)}_welfare_{welfare_type}_rep_{reporting_level}.csv",
            index=False,
        )

    log.info(f"Regional data extracted for {popshare_or_povline} = {value} ({ppp_version} PPPs)")

    return df


#############################################
#                                           #
#               MAIN FUNCTION               #
#                                           #
#############################################

# GENERATE MAIN INDICATORS FILE


def generate_key_indicators():
    """
    Generate the main indicators file, from a set of poverty lines and PPP versions
    """
    start_time = time.time()

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
                fill_gaps=FILL_GAPS,
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

    end_time = time.time()
    elapsed_time = round(end_time - start_time, 2)
    print("Done. Execution time:", elapsed_time, "seconds")

    return df


def generate_key_indicators_concurrent():
    """
    Generate the main indicators file, from a set of poverty lines and PPP versions. Uses concurrent.futures to speed up the process.
    """
    start_time = time.time()

    povlines_dict = {
        2011: [100, 190, 320, 550, 1000, 2000, 3000, 4000],
        2017: [100, 215, 365, 685, 1000, 2000, 3000, 4000],
    }

    versions = pip_versions()

    def get_country_data(povline, ppp_version, versions):
        return pip_query_country(
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
        return pip_query_region(
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
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            tasks = []
            for ppp_version, povlines in povlines_dict.items():
                for povline in povlines:
                    task = executor.submit(get_country_data, povline, ppp_version, versions)
                    tasks.append(task)
            results = [task.result() for task in concurrent.futures.as_completed(tasks)]
            # Concatenate list of dataframes
            results = pd.concat(results, ignore_index=True)

        return results

    def concurrent_region_function():
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            tasks = []
            for ppp_version, povlines in povlines_dict.items():
                for povline in povlines:
                    task = executor.submit(get_region_data, povline, ppp_version, versions)
                    tasks.append(task)
            results = [task.result() for task in concurrent.futures.as_completed(tasks)]
            # Concatenate list of dataframes
            results = pd.concat(results, ignore_index=True)

        return results

    # Run the main function
    results = concurrent_function()
    results_region = concurrent_region_function()

    # I check if the set of countries is the same in the df and in the aux table (list of countries)
    aux_dict = pip_aux_tables(table="countries")
    assert set(results["country"].unique()) == set(aux_dict["countries"]["country_name"].unique()), log.fatal(
        "List of countries is not the same!"
    )

    # I check if the set of regions is the same in the df and in the aux table (list of regions)
    aux_dict = pip_aux_tables(table="regions")
    assert set(results_region["country"].unique()) == set(aux_dict["regions"]["region"].unique()), log.fatal(
        "List of regions is not the same!"
    )

    # Concatenate df_country and df_region
    df = pd.concat([results, results_region], ignore_index=True)

    # Sort ppp_version, country, year and poverty_line
    df = df.sort_values(by=["ppp_version", "country", "year", "poverty_line"])

    # Save to csv
    df.to_csv(f"{PARENT_DIR}/pip.csv", index=False)

    end_time = time.time()
    elapsed_time = round(end_time - start_time, 2)
    print("Done. Execution time:", elapsed_time, "seconds")

    return df


# GENERATE RELATIVE POVERTY INDICATORS FILE
# This is data not given directly by the query, but we can get it by calculating 40, 50, 60% of the median and query
# NOTE: Medians need to be patched first in order to get data for all country-years (there are several missing values)


def generate_relative_poverty():
    """
    Generates relative poverty indicators from query results
    """
    start_time = time.time()

    versions = pip_versions()
    # Get data from the most common query
    df = pip_query_country(
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

    for pct in [40, 50, 60]:
        # Initialize lists
        headcount_ratio_list = []
        pgi_list = []
        pov_severity_list = []
        watts_list = []
        for i in range(len(df)):
            if ~np.isnan(df["median"].iloc[i]):
                df_relative = pip_query_country(
                    popshare_or_povline="povline",
                    value=df["median"].iloc[i] * pct / 100,
                    versions=versions,
                    country_code=df["country_code"].iloc[i],
                    year=df["year"].iloc[i],
                    fill_gaps=FILL_GAPS,
                    welfare_type=df["welfare_type"].iloc[i],
                    reporting_level=df["reporting_level"].iloc[i],
                    ppp_version=2017,
                )

                headcount_ratio_value = df_relative["headcount"][0]
                headcount_ratio_list.append(headcount_ratio_value)
                pgi_value = df_relative["poverty_gap"][0]
                pgi_list.append(pgi_value)
                pov_severity_value = df_relative["poverty_severity"][0]
                pov_severity_list.append(pov_severity_value)
                watts_value = df_relative["watts"][0]
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

    # Save to csv
    df.to_csv(f"{PARENT_DIR}/pip_relative.csv", index=False)

    end_time = time.time()
    elapsed_time = round(end_time - start_time, 2)
    print("Done. Execution time:", elapsed_time, "seconds")


# Concurrent version of the function
# GENERATE RELATIVE POVERTY INDICATORS FILE
# This is data not given directly by the query, but we can get it by calculating 40, 50, 60% of the median and query
# NOTE: Medians need to be patched first in order to get data for all country-years (there are several missing values)


def generate_relative_poverty_concurrent():
    """
    Generates relative poverty indicators from query results. Uses concurrent.futures to speed up the process.
    """
    start_time = time.time()

    versions = pip_versions()
    # Get data from the most common query
    df = pip_query_country(
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

    def get_relative_data(df_row, pct, versions):
        if ~np.isnan(df_row["median"]):
            return pip_query_country(
                popshare_or_povline="povline",
                value=df_row["median"] * pct / 100,
                versions=versions,
                country_code=df_row["country_code"],
                year=df_row["year"],
                fill_gaps=FILL_GAPS,
                welfare_type=df_row["welfare_type"],
                reporting_level=df_row["reporting_level"],
                ppp_version=2017,
                download="false",
            )

    def concurrent_relative_function():
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            tasks = []
            for pct in [40, 50, 60]:
                for i in range(len(df)):
                    task = executor.submit(get_relative_data, df.iloc[i], pct, versions)
                    tasks.append(task)
            results = [task.result() for task in concurrent.futures.as_completed(tasks)]
            # Concatenate list of dataframes
            results = pd.concat(results, ignore_index=True)

        return results

    # Run the main function
    results = concurrent_relative_function()

    for pct in [40, 50, 60]:
        # Initialize lists
        headcount_ratio_list = []
        pgi_list = []
        pov_severity_list = []
        watts_list = []
        for i in range(len(df)):
            if ~np.isnan(df["median"].iloc[i]):
                povline = round(df["median"].iloc[i] * pct / 100, 2)

                mask = (
                    (results["poverty_line"] == povline)
                    & (results["country"] == df["country"].iloc[i])
                    & (results["year"] == df["year"].iloc[i])
                    & (results["welfare_type"] == df["welfare_type"].iloc[i])
                    & (results["reporting_level"] == df["reporting_level"].iloc[i])
                )

                headcount_ratio_value = results[mask]["headcount"].iloc[0]
                headcount_ratio_list.append(headcount_ratio_value)

                pgi_value = results[mask]["poverty_gap"].iloc[0]
                pgi_list.append(pgi_value)

                pov_severity_value = results[mask]["poverty_severity"].iloc[0]
                pov_severity_list.append(pov_severity_value)

                watts_value = results[mask]["watts"].iloc[0]
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

    end_time = time.time()
    elapsed_time = round(end_time - start_time, 2)
    print("Done. Execution time:", elapsed_time, "seconds")


def generate_percentiles():
    """
    Generates percentiles data from query results
    """
    start_time = time.time()

    versions = pip_versions()

    # Define poverty lines and their increase

    under_5_dollars = list(range(1, 500, 1))
    between_5_and_10_dollars = list(range(500, 1000, 1))
    between_10_and_20_dollars = list(range(1000, 2000, 2))
    between_20_and_30_dollars = list(range(2000, 3000, 2))
    between_30_and_55_dollars = list(range(3000, 5500, 5))
    between_55_and_80_dollars = list(range(5500, 8000, 5))
    between_80_and_100_dollars = list(range(8000, 10000, 5))
    between_100_and_150_dollars = list(range(10000, 15000, 10))
    between_150_and_175_dollars = list(range(15000, 17500, 10))

    # povlines is all these lists toghether
    povlines = (
        under_5_dollars
        + between_5_and_10_dollars
        + between_10_and_20_dollars
        + between_20_and_30_dollars
        + between_30_and_55_dollars
        + between_55_and_80_dollars
        + between_80_and_100_dollars
        + between_100_and_150_dollars
        + between_150_and_175_dollars
    )

    for povline in povlines:
        # If file exists, skip
        if Path(f"{PARENT_DIR}/pip_country_data/pip_all_all_povline_{povline}_all_all.csv").is_file():
            continue
        else:
            pip_query_country(
                popshare_or_povline="povline",
                value=povline / 100,
                versions=versions,
                country_code="all",
                year="all",
                fill_gaps=FILL_GAPS,
                welfare_type="all",
                reporting_level="all",
                ppp_version=2017,
                download="true",
            )

    end_time = time.time()
    elapsed_time = round(end_time - start_time, 2)
    print("Done. Execution time:", elapsed_time, "seconds")


def generate_percentiles_concurrent():
    """
    Generates percentiles data from query results. Uses concurrent.futures to speed up the process.
    """
    start_time = time.time()

    versions = pip_versions()

    # Define poverty lines and their increase

    under_5_dollars = list(range(1, 500, 1))
    between_5_and_10_dollars = list(range(500, 1000, 1))
    between_10_and_20_dollars = list(range(1000, 2000, 2))
    between_20_and_30_dollars = list(range(2000, 3000, 2))
    between_30_and_55_dollars = list(range(3000, 5500, 5))
    between_55_and_80_dollars = list(range(5500, 8000, 5))
    between_80_and_100_dollars = list(range(8000, 10000, 5))
    between_100_and_150_dollars = list(range(10000, 15000, 10))
    between_150_and_175_dollars = list(range(15000, 17500, 10))

    # povlines is all these lists toghether
    povlines = (
        under_5_dollars
        + between_5_and_10_dollars
        + between_10_and_20_dollars
        + between_20_and_30_dollars
        + between_30_and_55_dollars
        + between_55_and_80_dollars
        + between_80_and_100_dollars
        + between_100_and_150_dollars
        + between_150_and_175_dollars
    )

    def get_percentiles_data(povline, versions):
        return pip_query_country(
            popshare_or_povline="povline",
            value=povline / 100,
            versions=versions,
            country_code="all",
            year="all",
            fill_gaps=FILL_GAPS,
            welfare_type="all",
            reporting_level="all",
            ppp_version=2017,
            download="true",
        )

    def concurrent_percentiles_function():
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            tasks = []
            for povline in povlines:
                task = executor.submit(get_percentiles_data, povline, versions)
                tasks.append(task)

            # I comment this because the output would be too large to handle
            # results = [task.result() for task in concurrent.futures.as_completed(tasks)]
            # # Concatenate list of dataframes
            # results = pd.concat(results, ignore_index=True)

        # return results

    # Run the main function
    concurrent_percentiles_function()

    end_time = time.time()
    elapsed_time = round(end_time - start_time, 2)
    print("Done. Execution time:", elapsed_time, "seconds")


# generate_key_indicators_concurrent()
# generate_relative_poverty_concurrent()
# generate_percentiles_concurrent()
