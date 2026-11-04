"""Script to create a snapshot of the foreign-born population of the United States by country
of birth, from the American Community Survey (ACS) 1-year estimates.

Fetches table B05006 ("Place of Birth for the Foreign-Born Population in the United States")
for the whole United States, for every year from 2005, through the Census Bureau data API.

The table's variable codes shift between years as countries are added or renamed, so the
snapshot stores, for every year, each variable code together with the API's own label for it
(e.g. "Estimate!!Total:!!Europe:!!Northern Europe:!!Denmark") and its value.

The API requires a (free) key: https://api.census.gov/data/key_signup.html
Set it as the US_CENSUS_API_KEY environment variable (e.g. in .env) before running.
"""

import os
import re

import pandas as pd
import requests

from etl.helpers import PathFinder

paths = PathFinder(__file__)

FIRST_YEAR = 2005
LAST_YEAR = 2024


def run(upload: bool = True) -> None:
    snap = paths.init_snapshot()

    api_key = os.environ["US_CENSUS_API_KEY"]

    rows = []
    for year in range(FIRST_YEAR, LAST_YEAR + 1):
        if year == 2020:
            # The Census Bureau did not release standard 1-year estimates for 2020 because of
            # data-collection problems during the COVID-19 pandemic.
            continue
        labels = requests.get(f"https://api.census.gov/data/{year}/acs/acs1/groups/B05006.json", timeout=60).json()
        labels = {
            code: meta["label"] for code, meta in labels["variables"].items() if re.fullmatch(r"B05006_\d+E", code)
        }
        response = requests.get(
            f"https://api.census.gov/data/{year}/acs/acs1",
            params={"get": "group(B05006)", "for": "us:1", "key": api_key},
            timeout=60,
        )
        response.raise_for_status()
        header, values = response.json()
        record = dict(zip(header, values))
        for code, label in labels.items():
            rows.append({"year": year, "code": code, "label": label, "value": record[code]})

    df = pd.DataFrame(rows)

    assert df["year"].nunique() == LAST_YEAR - FIRST_YEAR, "Unexpected number of years fetched (all years except 2020)."
    assert df.groupby("year").size().min() > 100, "Suspiciously few rows for some year."

    snap.create_snapshot(data=df, upload=upload)
