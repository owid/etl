"""Script to create a snapshot of the foreign-born population of the United States from the
American Community Survey (ACS) 1-year estimates.

Fetches table B05002 ("Place of Birth by Nativity and Citizenship Status") for the whole United States, for every
year from 2005, through the Census Bureau data API.

The API requires a (free) key: https://api.census.gov/data/key_signup.html
Set it as the US_CENSUS_API_KEY environment variable (e.g. in .env) before running.
"""

import os

import pandas as pd
import requests

from etl.helpers import PathFinder

paths = PathFinder(__file__)

FIRST_YEAR = 2005
LAST_YEAR = 2024

# B05002_001E is the total population; B05002_013E is the foreign-born population.
VARIABLES = ["B05002_001E", "B05002_013E"]


def run(upload: bool = True) -> None:
    snap = paths.init_snapshot()

    api_key = os.environ["US_CENSUS_API_KEY"]

    rows = []
    for year in range(FIRST_YEAR, LAST_YEAR + 1):
        if year == 2020:
            # The Census Bureau did not release standard 1-year estimates for 2020 because of
            # data-collection problems during the COVID-19 pandemic.
            continue
        response = requests.get(
            f"https://api.census.gov/data/{year}/acs/acs1",
            params={"get": ",".join(VARIABLES), "for": "us:1", "key": api_key},
            timeout=60,
        )
        response.raise_for_status()
        # Each response is a two-row JSON array: the header and the single row of values. Store
        # them exactly as returned (the year, taken from the request, distinguishes the rows).
        header, values = response.json()
        rows.append({"year": year, **dict(zip(header, values))})

    df = pd.DataFrame(rows)

    assert len(df) == LAST_YEAR - FIRST_YEAR, "Unexpected number of years fetched."
    assert (df["B05002_013E"].astype(int) < df["B05002_001E"].astype(int)).all(), (
        "Foreign-born exceeds total population."
    )

    snap.create_snapshot(data=df, upload=upload)
