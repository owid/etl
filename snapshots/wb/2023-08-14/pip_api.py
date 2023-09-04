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
MAX_WORKERS = 10

# NOTE: Although the number of workers is set to MAX_WORKERS, the actual number of workers for regional queries is half of that, because the API (`pip-grp`) is less able to handle concurrent requests.


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

    # Test health of the API
    api_health()

    # Round povline (popshare) to 2 decimals to work with cents as the minimum unit
    value = round(value, 2)

    # Extract version and release_version from versions dict
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
    assert repeat < MAX_REPEATS, log.fatal(
        f"Repeated {repeat} times, can't extract data ({popshare_or_povline} = {value}, {ppp_version} PPPs)"
    )

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
            f"{PARENT_DIR}/pip_country_data/pip_country_{country_code}_year_{year}_{popshare_or_povline}_{int(round(value*100))}_welfare_{welfare_type}_rep_{reporting_level}_fillgaps_{fill_gaps}_ppp_{ppp_version}.csv",
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

    # Test health of the API
    api_health()

    # Round povline (popshare) to 2 decimals to work with cents as the minimum unit
    value = round(value, 2)

    # Extract version and release_version from versions dict
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
    assert repeat < MAX_REPEATS, log.fatal(
        f"Repeated {repeat} times, can't extract data ({popshare_or_povline} = {value}, {ppp_version} PPPs)"
    )

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
            f"{PARENT_DIR}/pip_region_data/pip_region_{country_code}_year_{year}_{popshare_or_povline}_{int(round(value*100))}_ppp_{ppp_version}.csv",
            index=False,
        )

    if country_code == "all":
        log.info(f"Regional data extracted for {popshare_or_povline} = {value} ({ppp_version} PPPs)")
    else:
        log.info(
            f"Regional data extracted for {popshare_or_povline} = {value} ({ppp_version} PPPs) in {country_code} {year}"
        )

    return df


# GENERATE MAIN INDICATORS FILE


def generate_key_indicators():
    """
    Generate the main indicators file, from a set of poverty lines and PPP versions. Uses concurrent.futures to speed up the process.
    """
    start_time = time.time()

    # Define poverty lines, depending on the PPP version. It includes the international poverty line, lower and upper-middle income lines, and some other lines.
    povlines_dict = {
        2011: [100, 190, 320, 550, 1000, 2000, 3000, 4000],
        2017: [100, 215, 365, 685, 1000, 2000, 3000, 4000],
    }

    def get_country_data(povline, ppp_version, versions):
        """
        This function is defined inside the main function because it needs to be called by concurrent.futures.
        For country data.
        """
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
        """
        This function is defined inside the main function because it needs to be called by concurrent.futures.
        For regional data.
        """
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
        """
        This function makes concurrency work for country data.
        """
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
        """
        This function makes concurrency work for regional data.
        """
        with concurrent.futures.ThreadPoolExecutor(max_workers=int(round(MAX_WORKERS / 2))) as executor:
            tasks = []
            for ppp_version, povlines in povlines_dict.items():
                for povline in povlines:
                    task = executor.submit(get_region_data, povline, ppp_version, versions)
                    tasks.append(task)
            results = [task.result() for task in concurrent.futures.as_completed(tasks)]
            # Concatenate list of dataframes
            results = pd.concat(results, ignore_index=True)

        return results

    # Obtain latest versions of the PIP dataset
    versions = pip_versions()

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
    df.to_csv(f"{PARENT_DIR}/pip_raw.csv", index=False)

    end_time = time.time()
    elapsed_time = round(end_time - start_time, 2)
    print("Done. Execution time:", elapsed_time, "seconds")

    return df


# GENERATE RELATIVE POVERTY INDICATORS FILE
# This is data not given directly by the query, but we can get it by calculating 40, 50, 60% of the median and query
# NOTE: Medians need to be patched first in order to get data for all country-years (there are several missing values)


def generate_relative_poverty():
    """
    Generates relative poverty indicators from query results. Uses concurrent.futures to speed up the process.
    """
    start_time = time.time()

    def get_relative_data(df_row, pct, versions):
        """
        This function is structured in a way to make it work with concurrent.futures.
        It processes country data.
        """
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
                download="true",
            )

    def concurrent_relative_function():
        """
        This is the main function to make concurrency work for country data.
        """
        # Make sure the directory exists. If not, create it
        Path(f"{PARENT_DIR}/pip_country_data").mkdir(parents=True, exist_ok=True)
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for pct in [40, 50, 60]:
                for i in range(len(df)):
                    if Path(
                        f"{PARENT_DIR}/pip_country_data/pip_country_{df.iloc[i]['country_code']}_year_{df.iloc[i]['year']}_povline_{int(round(df.iloc[i]['median']*pct))}_welfare_{df.iloc[i]['welfare_type']}_rep_{df.iloc[i]['reporting_level']}_fillgaps_{FILL_GAPS}_ppp_2017.csv"
                    ).is_file():
                        continue
                    else:
                        executor.submit(get_relative_data, df.iloc[i], pct, versions)

    def get_relative_data_region(df_row, pct, versions):
        """
        This function is structured in a way to make it work with concurrent.futures.
        It processes regional data.
        """
        if ~np.isnan(df_row["median"]):
            return pip_query_region(
                popshare_or_povline="povline",
                value=df_row["median"] * pct / 100,
                versions=versions,
                country_code=df_row["country_code"],
                year=df_row["year"],
                welfare_type=df_row["welfare_type"],
                reporting_level=df_row["reporting_level"],
                ppp_version=2017,
                download="true",
            )

    def concurrent_relative_region_function():
        """
        This is the main function to make concurrency work for regional data.
        """
        # Make sure the directory exists. If not, create it
        Path(f"{PARENT_DIR}/pip_region_data").mkdir(parents=True, exist_ok=True)
        with concurrent.futures.ThreadPoolExecutor(max_workers=int(round(MAX_WORKERS / 2))) as executor:
            for pct in [40, 50, 60]:
                for i in range(len(df)):
                    if Path(
                        f"{PARENT_DIR}/pip_region_data/pip_region_{df.iloc[i]['country_code']}_year_{df.iloc[i]['year']}_povline_{int(round(df.iloc[i]['median']*pct))}_ppp_2017.csv"
                    ).is_file():
                        continue
                    else:
                        executor.submit(get_relative_data_region, df.iloc[i], pct, versions)

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
                        file_path = f"{PARENT_DIR}/pip_country_data/pip_country_{df.iloc[i]['country_code']}_year_{df.iloc[i]['year']}_povline_{int(round(df.iloc[i]['median']*pct))}_welfare_{df.iloc[i]['welfare_type']}_rep_{df.iloc[i]['reporting_level']}_fillgaps_{FILL_GAPS}_ppp_2017.csv"
                        if Path(file_path).is_file():
                            results = pd.read_csv(file_path)
                        else:
                            # Run the main function to get the data
                            concurrent_relative_function()
                            results = pd.read_csv(file_path)

                    elif country_or_region == "region":
                        # Here I check if the file exists even after the original extraction. If it does, I read it. If not, I start the queries again.
                        file_path = f"{PARENT_DIR}/pip_region_data/pip_region_{df.iloc[i]['country_code']}_year_{df.iloc[i]['year']}_povline_{int(round(df.iloc[i]['median']*pct))}_ppp_2017.csv"
                        if Path(file_path).is_file():
                            results = pd.read_csv(file_path)
                        else:
                            # Run the main function to get the data
                            concurrent_relative_region_function()
                            results = pd.read_csv(file_path)

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
    versions = pip_versions()

    # FOR COUNTRIES
    # Get data from the most common query
    df_country = pip_query_country(
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
    df_country = median_patch(df_country)

    # Run the main function to get the data
    concurrent_relative_function()

    # Add relative indicators from the results above
    df_country = add_relative_indicators(df=df_country, country_or_region="country")

    # FOR REGIONS
    # Get data from the most common query
    df_region = pip_query_region(
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
    df_region = median_patch(df_region)

    # Run the main function to get the data
    concurrent_relative_region_function()

    # Add relative indicators from the results above
    df_region = add_relative_indicators(df=df_region, country_or_region="region")

    # Concatenate df_country and df_region
    df = pd.concat([df_country, df_region], ignore_index=True)

    # Save to csv
    df.to_csv(f"{PARENT_DIR}/pip_relative.csv", index=False)

    end_time = time.time()
    elapsed_time = round(end_time - start_time, 2)
    print("Done. Execution time:", elapsed_time, "seconds")

    return df


# GENERATE PERCENTILES FILES
# This is data not given directly by the query, but we can get it by querying a huge set of poverty lines and assign percentiles according to headcount ratio results.


def generate_percentiles_raw():
    """
    Generates percentiles data from query results. This is the raw data to get the percentiles.
    Uses concurrent.futures to speed up the process.
    """
    start_time = time.time()

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

    def get_percentiles_data(povline, versions, ppp_version):
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
            download="true",
        )

    def concurrent_percentiles_function():
        # Make sure the directory exists. If not, create it
        Path(f"{PARENT_DIR}/pip_country_data").mkdir(parents=True, exist_ok=True)
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for ppp_version in [2011, 2017]:
                for povline in povlines:
                    if Path(
                        f"{PARENT_DIR}/pip_country_data/pip_country_all_year_all_povline_{povline}_welfare_all_rep_all_fillgaps_{FILL_GAPS}_ppp_{ppp_version}.csv"
                    ).is_file():
                        continue
                    else:
                        executor.submit(get_percentiles_data, povline, versions, ppp_version)

    def get_percentiles_data_region(povline, versions, ppp_version):
        return pip_query_region(
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
        # Make sure the directory exists. If not, create it
        Path(f"{PARENT_DIR}/pip_region_data").mkdir(parents=True, exist_ok=True)
        with concurrent.futures.ThreadPoolExecutor(max_workers=int(round(MAX_WORKERS / 2))) as executor:
            for ppp_version in [2011, 2017]:
                for povline in povlines:
                    if Path(
                        f"{PARENT_DIR}/pip_region_data/pip_region_all_year_all_povline_{povline}_ppp_{ppp_version}.csv"
                    ).is_file():
                        continue
                    else:
                        executor.submit(get_percentiles_data_region, povline, versions, ppp_version)

    # Obtain latest versions of the PIP dataset
    versions = pip_versions()

    # Run the main function
    concurrent_percentiles_function()
    concurrent_percentiles_region_function()

    # Concatenate country and region files
    df_country = pd.DataFrame()
    df_region = pd.DataFrame()

    for ppp_version in [2011, 2017]:
        for povline in povlines:
            # Here I check if the file exists even after the original extraction. If it does, I read it. If not, I start the queries again.
            file_path_country = f"{PARENT_DIR}/pip_country_data/pip_country_all_year_all_povline_{povline}_welfare_all_rep_all_fillgaps_{FILL_GAPS}_ppp_{ppp_version}.csv"
            if Path(file_path_country).is_file():
                df_query_country = pd.read_csv(file_path_country)
            else:
                # Run the main function to get the data
                concurrent_percentiles_function()
                df_query_country = pd.read_csv(file_path_country)

            df_country = pd.concat([df_country, df_query_country], ignore_index=True)

            # Here I check if the file exists even after the original extraction. If it does, I read it. If not, I start the queries again.
            file_path_region = (
                f"{PARENT_DIR}/pip_region_data/pip_region_all_year_all_povline_{povline}_ppp_{ppp_version}.csv"
            )
            if Path(file_path_region).is_file():
                df_query_region = pd.read_csv(file_path_region)
            else:
                # Run the main function to get the data
                concurrent_percentiles_region_function()
                df_query_region = pd.read_csv(file_path_region)

            df_region = pd.concat([df_region, df_query_region], ignore_index=True)

    # I check if the set of countries is the same in the df and in the aux table (list of countries)
    aux_dict = pip_aux_tables(table="countries")
    assert set(df_country["country"].unique()) == set(aux_dict["countries"]["country_name"].unique()), log.fatal(
        "List of countries is not the same as the one defined in PIP!"
    )

    # I check if the set of regions is the same in the df and in the aux table (list of regions)
    aux_dict = pip_aux_tables(table="regions")
    assert set(df_region["country"].unique()) == set(aux_dict["regions"]["region"].unique()), log.fatal(
        "List of regions is not the same as the one defined in PIP!"
    )

    # Concatenate df_country and df_region
    df = pd.concat([df_country, df_region], ignore_index=True)

    end_time = time.time()
    elapsed_time = round(end_time - start_time, 2)
    print("Done. Execution time:", elapsed_time, "seconds")

    return df


def generate_consolidated_percentiles(df):
    """
    Generates percentiles from the raw data. This is the final file with percentiles.
    """

    start_time = time.time()

    # Define percentiles, from 1 to 99
    percentiles = range(1, 100, 1)
    df_percentiles = pd.DataFrame()

    for p in percentiles:
        df["distance_to_p"] = abs(df["headcount"] - p / 100)
        df_closest = (
            df.sort_values("distance_to_p")
            .groupby(["country", "year", "reporting_level", "welfare_type"], as_index=False)
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
        df_percentiles = pd.concat([df_percentiles, df_closest], ignore_index=True)

    # Rename headcount to estimated_percentile and poverty_line to thr
    df_percentiles = df_percentiles.rename(columns={"headcount": "estimated_percentile", "poverty_line": "thr"})

    # Sort by ppp_version, country, year, reporting_level, welfare_type and target_percentile
    df_percentiles = df_percentiles.sort_values(
        by=["ppp_version", "country", "year", "reporting_level", "welfare_type", "target_percentile"]
    )

    # Save to csv
    df_percentiles.to_csv(f"{PARENT_DIR}/pip_percentiles.csv", index=False)

    end_time = time.time()
    elapsed_time = round(end_time - start_time, 2)
    print("Done. Execution time:", elapsed_time, "seconds")

    return df_percentiles


def median_patch(df):
    """
    Patch missing values in the median column.
    PIP queries do not return all the medians, so they are patched with the results of the percentile file.
    """

    # Read percentile file
    df_percentiles = pd.read_csv(f"{PARENT_DIR}/pip_percentiles.csv")

    # In df_percentiles, keep only the rows with target_percentile = 50
    df_percentiles = df_percentiles[df_percentiles["target_percentile"] == 50].reset_index()

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

    return df


def add_relative_poverty_and_decile_threholds(df, df_relative, df_percentiles):
    """
    Add relative poverty indicators and decile thresholds to the key indicators file.
    """

    # Add relative poverty indicators
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

    # In df_percentiles, keep only the rows with target_percentile = 10, 20, 30, ... 90
    df_percentiles = df_percentiles[df_percentiles["target_percentile"] % 10 == 0].reset_index()

    # Make tb_percentile wide, with target_percentile as columns
    df_percentiles = df_percentiles.pivot(
        index=["ppp_version", "country", "year", "reporting_level", "welfare_type"],
        columns="target_percentile",
        values="thr",
    )

    # Flatten column names
    df_percentiles.columns = ["".join(col).strip() for col in df_percentiles.columns.values]

    # Reset index
    df_percentiles = df_percentiles.reset_index()

    # Replace column names from thr to decile
    df_percentiles = df_percentiles.rename(
        columns={
            "thr10": "decile1_thr",
            "thr20": "decile2_thr",
            "thr30": "decile3_thr",
            "thr40": "decile4_thr",
            "thr50": "decile5_thr",
            "thr60": "decile6_thr",
            "thr70": "decile7_thr",
            "thr80": "decile8_thr",
            "thr90": "decile9_thr",
        }
    )

    # Merge df and df_percentiles
    df = pd.merge(
        df,
        df_percentiles,
        on=["ppp_version", "country", "year", "reporting_level", "welfare_type"],
        how="left",
    )

    # Save key indicators file
    df.to_csv(f"{PARENT_DIR}/world_bank_pip.csv", index=False)

    return df


#############################################
#                                           #
#               MAIN FUNCTION               #
#                                           #
#############################################

# Generate percentiles by extracting the raw files and processing them afterward
df_percentiles = generate_consolidated_percentiles(generate_percentiles_raw())

# Generate relative poverty indicators file
df_relative = generate_relative_poverty()

# Generate key indicators file and patch medians
df = generate_key_indicators()
df = median_patch(df)

# Add relative poverty indicators and decile thresholds to the key indicators file
df = add_relative_poverty_and_decile_threholds(df, df_relative, df_percentiles)
