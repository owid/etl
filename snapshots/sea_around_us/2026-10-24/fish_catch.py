"""Script to create a snapshot of Sea Around Us reconstructed marine catch, by fishing country and globally.

The data comes from the Sea Around Us API (no login needed). For the world and for every fishing country
("fishing entity" in Sea Around Us terms: the country whose fleet does the fishing), it fetches the yearly catch
in tonnes broken down along five dimensions:
- enduse: direct human consumption, fishmeal and fish oil, other.
- gear: bottom trawl, pelagic trawl, purse seine, longline, gillnets, small scale, other, unknown.
- sector: industrial, artisanal, subsistence, recreational.
- catchtype: landings, discards.
- reporting-status: reported, unreported.

The rows are stored as the API returns them (one row per area, dimension, category and year).

NOTE: The API answers 404 for fishing entities that have no catch of their own (e.g. "European Union", "Mariculture",
or sub-national entities like "Alaska (USA)" whose catch sits under the parent country). Those are skipped, but the
number of skipped entities is checked, so that a change in the API's coverage gets noticed.
"""

from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import requests
from structlog import get_logger

from etl.helpers import PathFinder

log = get_logger()

paths = PathFinder(__file__)

# Base URL of the Sea Around Us API.
API_URL = "https://api.seaaroundus.org/api/v1"

# Dimensions along which the catch is broken down.
DIMENSIONS = ["enduse", "gear", "sector", "catchtype", "reporting-status"]

# Region id of the whole world in the "global" endpoint.
GLOBAL_REGION_ID = 1

# Maximum number of fishing entities without data (19 at the time of writing).
MAX_ENTITIES_WITHOUT_DATA = 25

# The API is slow (~5 s per request), so requests are run in parallel.
MAX_WORKERS = 8
TIMEOUT = 180
RETRIES = 3


def run(upload: bool = True) -> None:
    snap = paths.init_snapshot()

    # Step 1: List all fishing entities.
    entities = fetch_fishing_entities()

    # Step 2: Fetch the global catch, and the catch of each fishing entity, for each dimension.
    requests_to_make = [("global", GLOBAL_REGION_ID, "Global", dimension) for dimension in DIMENSIONS] + [
        ("fishing-entity", entity["id"], entity["title"], dimension) for entity in entities for dimension in DIMENSIONS
    ]
    with ThreadPoolExecutor(MAX_WORKERS) as executor:
        results = list(executor.map(lambda args: fetch_tonnage(*args), requests_to_make))
    df = pd.concat([result for result in results if result is not None], ignore_index=True)

    # Step 3: Check the result.
    sanity_check(df, entities)

    snap.create_snapshot(data=df, upload=upload)


def get_json(url: str, params: dict | None = None) -> dict | None:
    """Get a JSON response from the API, retrying on errors. Return None if the API says there is no data (404)."""
    for attempt in range(RETRIES):
        try:
            response = requests.get(url, params=params, timeout=TIMEOUT)
            if response.status_code == 404:
                return None
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            if attempt == RETRIES - 1:
                raise e
            log.warning(f"Retrying {url} {params} after error: {e}")


def fetch_fishing_entities() -> list[dict]:
    data = get_json(f"{API_URL}/fishing-entity/")
    assert data is not None, "The list of fishing entities is not available."
    return data["data"]


def fetch_tonnage(area_type: str, region_id: int, area_name: str, dimension: str) -> pd.DataFrame | None:
    data = get_json(f"{API_URL}/{area_type}/tonnage/{dimension}/", params={"region_id": region_id})
    if data is None:
        return None
    rows = [
        {
            "area_type": area_type,
            "area_id": region_id,
            "area_name": area_name,
            "dimension": dimension,
            "category": series["key"],
            "year": year,
            "tonnes": value,
        }
        for series in data["data"]
        for year, value in series["values"]
    ]
    return pd.DataFrame(rows)


def sanity_check(df: pd.DataFrame, entities: list[dict]) -> None:
    # Every dimension should be present for the world.
    dimensions_global = set(df[df["area_type"] == "global"]["dimension"])
    assert dimensions_global == set(DIMENSIONS), f"Missing global dimensions: {set(DIMENSIONS) - dimensions_global}"

    # Only a handful of fishing entities should have no data.
    entities_with_data = set(df[df["area_type"] == "fishing-entity"]["area_id"])
    entities_without_data = sorted(entity["title"] for entity in entities if entity["id"] not in entities_with_data)
    log.info(f"Fishing entities without data: {entities_without_data}")
    assert len(entities_without_data) <= MAX_ENTITIES_WITHOUT_DATA, (
        f"{len(entities_without_data)} fishing entities have no data: {entities_without_data}"
    )

    # The data should start in 1950, and there should be no duplicates.
    assert df["year"].min() == 1950, f"Unexpected first year: {df['year'].min()}"
    assert not df.duplicated(subset=["area_type", "area_id", "dimension", "category", "year"]).any(), "Duplicates."
