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


def api_aux_tables(health):
    """
    Download aux tables if the API is running.
    """
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


def api_versions(health) -> dict:
    """
    Download aux tables if the API is running.
    """
    if health == "PIP API is running":
        log.info(f"Health check: {health}")

        df = pd.read_csv("https://api.worldbank.org/pip/v1/versions?format=csv")

        # Obtain the max release_version
        max_release_version = df["release_version"].max()

        # Get the version for ppp_versions 2011 and 2017
        ppp_versions = (
            df[df["release_version"] == max_release_version][["ppp_version", "release_version", "version"]]
            .set_index()
            .to_dict()
        )

    else:
        log.error(f"Health check: {health}")

    return ppp_versions


ppp_versions = api_versions(health=api_health())
print(f"{ppp_versions[0]}")
