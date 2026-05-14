"""Equaldex data extraction script.

This script extracts data from the Equaldex JSON API and uploads three
snapshots directly:

- ``equaldex.csv``           — long-format historical + current data
                               (one row per country/year/issue/value).
- ``equaldex_current.csv``   — current status per country/issue.
- ``equaldex_indices.csv``   — equality, legal and public-opinion indices
                               per country (one row per country per
                               extraction year).

An Equaldex API key is required. Add it to ``.env`` at the repo root:

    EQUALDEX_KEY="your_api_key"

You can obtain one by registering at https://www.equaldex.com/ and copying
the key from https://www.equaldex.com/settings.

To run:

    etls lgbt_rights/{version}/equaldex_extract
    # or in the background:
    nohup uv run etls lgbt_rights/{version}/equaldex_extract > out.log 2>&1 &

The full run hits the Equaldex API once per region listed in
``country_list.json`` (a few hundred calls) and typically takes a few
minutes. Per-country responses are cached on disk via ``joblib.Memory`` so
re-runs after a transient failure skip already-fetched regions.
"""

import datetime
import json
import os
from pathlib import Path

import click
import pandas as pd
import requests
from joblib import Memory
from structlog import get_logger
from tenacity import retry
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_random_exponential
from tqdm import tqdm

from etl.paths import CACHE_DIR
from etl.snapshot import Snapshot

log = get_logger()

# Resolve namespace/version from the script location.
PARENT_DIR = Path(__file__).parent.absolute()
SNAPSHOT_VERSION = PARENT_DIR.name

# Persistent cache so we don't re-hit the API on retries.
memory = Memory(CACHE_DIR / "equaldex", verbose=0)

# API parameters.
API_URL = "https://www.equaldex.com/api/region"
TIMEOUT_SECONDS = 60
MAX_REPEATS = 8

# Regex to find a year inside ``start_date_formatted`` / ``end_date_formatted``.
YEAR_REGEX = r"(\d{4})"  # ty: ignore

# Start year used when expanding the current status backwards in time when no
# explicit ``start_date_formatted`` is provided by the source.
START_YEAR = 1950

# Fields we care about for the per-country indices table.
VARIABLES_INDICES = ["name", "ei", "ei_legal", "ei_po"]


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
def main(upload: bool) -> None:
    api_key = os.getenv("EQUALDEX_KEY")
    if not api_key:
        raise click.ClickException(
            "EQUALDEX_KEY not set. Add it to the repo .env (see this script's docstring)."
        )

    # Load the list of regions Equaldex publishes (curated alongside this script).
    with open(PARENT_DIR / "country_list.json") as f:
        country_list: list[str] = json.load(f)

    # Fetch and combine data.
    df_current, df_historical, df_indices = extract_from_api(country_list, api_key)
    df_long = create_long_dataset(df_current, df_historical)

    # Upload the three snapshots directly.
    Snapshot(f"lgbt_rights/{SNAPSHOT_VERSION}/equaldex.csv").create_snapshot(data=df_long, upload=upload)
    log.info("Uploaded snapshot: equaldex.csv")

    Snapshot(f"lgbt_rights/{SNAPSHOT_VERSION}/equaldex_current.csv").create_snapshot(
        data=df_current, upload=upload
    )
    log.info("Uploaded snapshot: equaldex_current.csv")

    Snapshot(f"lgbt_rights/{SNAPSHOT_VERSION}/equaldex_indices.csv").create_snapshot(
        data=df_indices, upload=upload
    )
    log.info("Uploaded snapshot: equaldex_indices.csv")


@retry(wait=wait_random_exponential(multiplier=1), stop=stop_after_attempt(MAX_REPEATS))
def _get_region(region_id: str, api_key: str) -> dict:
    """Fetch a single region's payload from the Equaldex API with retry."""
    response = requests.get(
        API_URL,
        headers={"Content-Type": "application/json"},
        params={"regionid": region_id, "apiKey": api_key},
        timeout=TIMEOUT_SECONDS,
    )
    if response.status_code != 200:
        raise Exception(f"Equaldex API returned status {response.status_code} for {region_id}")
    return json.loads(response.content)


@memory.cache(ignore=["api_key"])
def fetch_region(region_id: str, api_key: str) -> dict:
    """Cached wrapper around the per-region API call. Cache key is region_id."""
    return _get_region(region_id, api_key)


def extract_from_api(
    country_list: list[str], api_key: str
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Hit the Equaldex API for every region in ``country_list`` and assemble three frames."""
    current_year = datetime.datetime.now().year

    df_current = pd.DataFrame()
    df_historical = pd.DataFrame()
    df_indices = pd.DataFrame()

    countries_no_data: list[str] = []

    for region_id in tqdm(country_list, desc="Extracting data from countries"):
        payload = fetch_region(region_id, api_key)

        try:
            country_name = payload["regions"]["region"]["name"]
        except (KeyError, TypeError):
            continue

        try:
            issue_list = list(payload["regions"]["region"]["issues"].keys())
        except (KeyError, TypeError):
            countries_no_data.append(region_id)
            issue_list = []

        # Indices row for the region (one row per region).
        indices_row = pd.DataFrame()
        for variable in VARIABLES_INDICES:
            try:
                indices_row.loc[0, variable] = payload["regions"]["region"][variable]
            except (KeyError, TypeError):
                indices_row.loc[0, variable] = None

        indices_row = indices_row.dropna(axis=1, how="all")
        if not indices_row.isnull().all().all():
            df_indices = pd.concat([df_indices, indices_row], ignore_index=True)

        for issue in issue_list:
            issue_data = payload["regions"]["region"]["issues"][issue]

            try:
                current_data = pd.DataFrame(issue_data["current_status"], index=[0])
            except (KeyError, TypeError):
                log.warning(f"{country_name}: No current data for {issue}")
                current_data = pd.DataFrame()

            try:
                historical_data = pd.DataFrame(issue_data["history"])
            except (KeyError, TypeError):
                historical_data = pd.DataFrame()

            current_data["country"] = country_name
            historical_data["country"] = country_name
            current_data["issue"] = issue
            historical_data["issue"] = issue

            df_historical = pd.concat([df_historical, historical_data], ignore_index=True)
            df_current = pd.concat([df_current, current_data], ignore_index=True)

    if countries_no_data:
        log.info(
            f"Data was not found for the following {len(countries_no_data)} countries: \n{countries_no_data}"
        )

    cols_to_move = ["country", "issue"]
    df_current = df_current[cols_to_move + [c for c in df_current.columns if c not in cols_to_move]]
    df_historical = df_historical[
        cols_to_move + [c for c in df_historical.columns if c not in cols_to_move]
    ]

    df_current["year_extraction"] = current_year
    df_indices["year"] = current_year

    return df_current, df_historical, df_indices


def create_long_dataset(df_current: pd.DataFrame, df_historical: pd.DataFrame) -> pd.DataFrame:
    """Expand the per-issue history into one row per country/year/issue.

    Concatenates historical and current data, dedupes on (country, year, issue)
    and clips to ``START_YEAR`` onward. Mirrors the v1 layout exactly so the
    meadow/garden steps don't need to change.
    """
    current_year = datetime.datetime.now().year

    # HISTORICAL DATA
    df_historical = df_historical[
        ~df_historical["start_date_formatted"].isnull()
        & ~df_historical["end_date_formatted"].isnull()
    ].reset_index(drop=True)

    df_historical["year_start"] = (
        df_historical["start_date_formatted"].str.extract(YEAR_REGEX, expand=False).astype(int)
    )
    df_historical["year_end"] = (
        df_historical["end_date_formatted"].str.extract(YEAR_REGEX, expand=False).astype(int)
    )

    df_historical_long = pd.DataFrame()
    for i in range(len(df_historical)):
        df_country_issue = pd.DataFrame(
            {
                "country": df_historical.iloc[i]["country"],
                "year": range(
                    df_historical.iloc[i]["year_start"],
                    df_historical.iloc[i]["year_end"],
                ),
                "issue": df_historical.iloc[i]["issue"],
                "id": df_historical.iloc[i]["id"],
                "value": df_historical.iloc[i]["value"],
                "value_formatted": df_historical.iloc[i]["value_formatted"],
                "description": df_historical.iloc[i]["description"],
            }
        )
        df_historical_long = pd.concat([df_historical_long, df_country_issue], ignore_index=True)

    df_historical_long["dataset"] = "historical"

    # CURRENT DATA
    df_current = df_current.copy()
    df_current.loc[df_current["start_date_formatted"].isnull(), "date_modified"] = True
    df_current.loc[df_current["start_date_formatted"].isnull(), "start_date_formatted"] = (
        f"Jan 1, {current_year}"
    )

    df_current["year_start"] = (
        df_current["start_date_formatted"].str.extract(YEAR_REGEX, expand=False).astype(int)
    )

    df_current_long = pd.DataFrame()
    for i in range(len(df_current)):
        df_country_issue = pd.DataFrame(
            {
                "country": df_current.iloc[i]["country"],
                "year": range(df_current.iloc[i]["year_start"], current_year + 1),
                "issue": df_current.iloc[i]["issue"],
                "id": df_current.iloc[i]["id"],
                "value": df_current.iloc[i]["value"],
                "value_formatted": df_current.iloc[i]["value_formatted"],
                "description": df_current.iloc[i]["description"],
                "date_modified": df_current.iloc[i]["date_modified"],
            }
        )
        df_current_long = pd.concat([df_current_long, df_country_issue], ignore_index=True)

    df_current_long["dataset"] = "current"

    df_long = pd.concat([df_current_long, df_historical_long], ignore_index=True)
    df_long = df_long[df_long["year"] >= START_YEAR]
    df_long["date_modified"] = df_long["date_modified"].fillna(False)
    df_long = df_long.drop_duplicates(subset=["country", "year", "issue", "id"], keep="first")
    df_long = df_long.sort_values(
        by=["country", "year", "issue", "date_modified", "dataset"], ascending=True
    )
    return df_long
