"""Script to create a snapshot of dataset.

The data comes from UNHCR's public population API (https://api.unhcr.org/docs/refugee-statistics.html), which serves
the same figures as the Refugee Data Finder download page. The download page sits behind a captcha, and its current
CSV no longer includes returned refugees and returned IDPs, which the API still provides.

The API is queried one year at a time (a single request for all years fails with a 502) and the records are stored
as returned, with the API's own column names.
"""

from pathlib import Path

import pandas as pd
import requests

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

API_URL = "https://api.unhcr.org/population/v1/population/"
FIRST_YEAR = 1951
# Latest year of the annual statistics (published mid-June of the following year).
LAST_YEAR = 2025


def fetch_year(year: int) -> list[dict]:
    params = {
        "limit": 100_000,
        "yearFrom": year,
        "yearTo": year,
        "coo_all": "true",
        "coa_all": "true",
    }
    response = requests.get(API_URL, params=params, timeout=120)
    response.raise_for_status()
    data = response.json()
    # Make sure we got every record of the year in one page.
    assert data["maxPages"] == 1, f"{year}: expected a single page, got {data['maxPages']}."
    return data["items"]


def run(upload: bool = True) -> None:
    # Initialize a new snapshot.
    snap = Snapshot(f"un/{SNAPSHOT_VERSION}/refugee_data.csv")

    # Fetch all years from the API.
    records = []
    for year in range(FIRST_YEAR, LAST_YEAR + 1):
        records.extend(fetch_year(year))
    df = pd.DataFrame.from_records(records)

    # Sanity checks: every year is present, and there are no years beyond LAST_YEAR.
    years = set(df["year"].unique())
    assert years == set(range(FIRST_YEAR, LAST_YEAR + 1)), f"Unexpected set of years: {sorted(years)}"
    assert not df.duplicated(["year", "coo_iso", "coa_iso"]).any(), "Duplicate origin-asylum-year rows."

    # Save snapshot.
    snap.create_snapshot(data=df, upload=upload)
