from pathlib import Path

import pandas as pd
from structlog import get_logger

# Initialize logger.
log = get_logger()

PARENT_DIR = Path(__file__).parent.absolute()


def api_health() -> str:
    """
    Check if the API is running and download aux tables if it is.
    """
    health = pd.read_json("https://api.worldbank.org/pip/v1/health-check")[0][0]

    return health


def pip_aux_tables():
    """
    Download aux tables if the API is running.
    """

    health = api_health()

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
    if health == "PIP API is running":
        log.info(f"Health check: {health}")

        for table in aux_tables_list:
            df = pd.read_csv(f"https://api.worldbank.org/pip/v1/aux?table={table}&long_format=false&format=csv")
            # create folder if not exists
            Path(f"{PARENT_DIR}/aux").mkdir(parents=True, exist_ok=True)
            df.to_csv(f"{PARENT_DIR}/aux/{table}.csv", index=False)

        log.info("Aux tables downloaded")

    else:
        log.error(f"Health check: {health}")


def pip_versions() -> dict:
    """
    Download aux tables if the API is running.
    """

    health = api_health()

    if health == "PIP API is running":
        log.info(f"Health check: {health}")

        df = pd.read_csv("https://api.worldbank.org/pip/v1/versions?format=csv")
        df = df[["ppp_version", "release_version", "version"]]

        # Obtain the max release_version
        max_release_version = df["release_version"].max()

        # Get the version for ppp_versions 2011 and 2017
        versions = df[df["release_version"] == max_release_version]

        # Set index and convert to dict
        versions = versions.set_index("ppp_version", verify_integrity=True).sort_index().to_dict(orient="index")

    else:
        log.error(f"Health check: {health}")

    log.info("PIP dataset versions extracted")

    return versions


def pip_query(
    popshare_or_povline,
    value,
    country_code="all",
    year="all",
    fill_gaps="true",
    welfare_type="all",
    reporting_level="all",
    ppp_version="2017",
) -> pd.DataFrame:
    """
    Query the PIP API.
    """

    health = api_health()
    versions = pip_versions()

    if health == "PIP API is running":
        log.info(f"Health check: {health}")

        if ppp_version in ["2011", "2017"]:
            # convert ppp_version to integer
            ppp_version = int(ppp_version)
            version = versions[ppp_version]["version"]
            release_version = versions[ppp_version]["release_version"]

            df = pd.read_csv(
                f"https://api.worldbank.org/pip/v1/pip?{popshare_or_povline}={value}&country={country_code}&year={year}&fill_gaps={fill_gaps}&welfare_type={welfare_type}&reporting_level={reporting_level}&ppp_version={ppp_version}&version={version}&release_version={release_version}&format=csv"
            )

        elif ppp_version == "all":
            df = pd.DataFrame()
            for ppp_version in versions.keys():
                version = versions[ppp_version]["version"]
                release_version = versions[ppp_version]["release_version"]

                df_ppp = pd.read_csv(
                    f"https://api.worldbank.org/pip/v1/pip?{popshare_or_povline}={value}&country={country_code}&year={year}&fill_gaps={fill_gaps}&welfare_type={welfare_type}&reporting_level={reporting_level}&ppp_version={ppp_version}&version={version}&release_version={release_version}&format=csv"
                )
                df_ppp["ppp_version"] = ppp_version
                df = pd.concat([df, df_ppp], ignore_index=True)

    else:
        log.error(f"Health check: {health}")

    log.info(f"PIP query executed for {popshare_or_povline}={value} (ppp_version={ppp_version})")

    return df


df = pip_query(
    popshare_or_povline="povline",
    value=2.15,
    country_code="all",
    year="all",
    fill_gaps="true",
    welfare_type="all",
    reporting_level="all",
    ppp_version="all",
)
