"""Script to create a snapshot of dataset 'United Nations Sustainable Development Goals (2024)'.
As well as a snapshot of the data we collect a snapshot of the dimensions and attributes of the data.
These often change as the dataset contains many different variables with many different dimensions and values/attributes.
"""

import datetime as dt
import json
import os
import tempfile
import time
from collections import defaultdict
from io import StringIO
from pathlib import Path
from typing import Any

import click
import numpy as np
import pandas as pd
import requests
import yaml
from structlog import get_logger

from etl.snapshot import Snapshot, SnapshotMeta

log = get_logger()


URL_METADATA = "https://unstats.un.org/sdgs/indicators/SDG_Updateinfo.xlsx"
MAX_RETRIES = 10
CHUNK_SIZE = 1024 * 1024 * 10
# Backoff between retries of a failed goal request (doubles per attempt, capped).
RETRY_BACKOFF_SECONDS = 10
MAX_RETRY_WAIT_SECONDS = 120
# Attempts for a whole-goal request before falling back to per-area batches.
WHOLE_GOAL_ATTEMPTS = 2
# Number of area codes per request in that fallback. 100 keeps each response comfortably
# below the API's buffer limit for the largest goal (goal 4 peaks at ~66 MB per batch).
AREA_CHUNK_SIZE = 100

SDG_DATA_API = "https://unstats.un.org/sdgapi"


# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"un/{SNAPSHOT_VERSION}/un_sdg.feather")

    # Download data from source.

    log.info("Creating metadata...")
    with tempfile.TemporaryDirectory() as temp_dir:
        log.info("Downloading unit descriptions...")
        unit_desc = attributes_description(snap)
        unit_desc = pd.DataFrame(unit_desc.items(), columns=["AttCode", "AttValue"])
        log.info("Adding unit descriptions to catalog...")
        Snapshot(f"un/{SNAPSHOT_VERSION}/un_sdg_unit.csv").create_snapshot(data=unit_desc, upload=True)

        log.info("Downloading dimension descriptions...")
        dim_desc = dimensions_description(snap)
        dim_file = os.path.join(temp_dir, "data.json")
        with open(dim_file, "w") as fp:
            json.dump(dim_desc, fp)

        log.info("Adding dimension descriptions to catalog...")
        Snapshot(f"un/{SNAPSHOT_VERSION}/un_sdg_dimension.json").create_snapshot(filename=dim_file, upload=True)

        # fetch the file locally
        log.info("Downloading data...")
        all_data = download_data(snap)
        log.info("Adding data to catalog...")
        Snapshot(f"un/{SNAPSHOT_VERSION}/un_sdg.feather").create_snapshot(data=all_data, upload=True)


def create_metadata(snap: Snapshot) -> SnapshotMeta:
    """Updating metadata in so it matches the UN SDG update log"""
    meta = snap.metadata
    meta_update = load_external_metadata()
    meta.name = meta_update["name"]
    assert meta.origin
    meta.origin.date_published = meta_update["publication_date"]
    meta.origin.date_accessed = str(dt.datetime.now().date())
    return meta


def load_yaml_metadata() -> dict:
    fpath = Path(__file__).parent / f"{Path(__file__).stem}.meta.yml"
    with open(fpath) as istream:
        meta = yaml.safe_load(istream)
    return meta


def load_external_metadata() -> dict:
    meta_orig = pd.read_excel(URL_METADATA)
    meta_orig.columns = ["updated", "detail"]
    pub_date = meta_orig["detail"].iloc[0].date()

    meta = {
        "name": f"United Nations Sustainable Development Goals - United Nations ({pub_date})",
        "publication_year": pub_date.year,
        "publication_date": f"{pub_date}",
    }
    return meta


def download_data(snap: Snapshot) -> pd.DataFrame:
    # retrieves all goal codes
    log.info("Retrieving SDG goal codes...")
    url = f"{SDG_DATA_API}/v1/sdg/Goal/List"
    res = requests.get(url)
    assert res.ok

    goals = res.json()
    goal_codes = [str(goal["code"]) for goal in goals]

    # retrieves all area codes
    log.info("Retrieving area codes...")
    url = f"{SDG_DATA_API}/v1/sdg/GeoArea/List"
    res = requests.get(url)
    assert res.ok
    areas = res.json()
    area_codes = [str(area["geoAreaCode"]) for area in areas]
    # retrieves csv with data for all codes and areas
    log.info("Retrieving data...")
    url = f"{SDG_DATA_API}/v1/sdg/Goal/DataCSV"
    all_data = []
    for goal in goal_codes:
        frames = download_goal(url=url, goal=goal, area_codes=area_codes)
        rows = sum(len(f) for f in frames)
        assert rows > 0, f"No rows returned for goal {goal}."
        log.info("Downloaded goal", goal=goal, rows=rows, requests=len(frames))
        all_data += frames

    # Every goal must have contributed data, otherwise we would silently publish a
    # snapshot missing entire goals.
    assert len(all_data) >= len(goal_codes), f"Downloaded {len(all_data)} responses for {len(goal_codes)} goals."
    all_df = pd.concat(all_data)
    all_df = all_df.reset_index()
    cols = all_df.columns
    # Converting all columns to string dtype as feather doesn't like object dtype
    all_df[cols] = all_df[cols].astype("str")
    all_df = pd.DataFrame(all_df)

    return all_df


def download_goal(url: str, goal: str, area_codes: list) -> list[pd.DataFrame]:
    """Download one goal, splitting the request across area codes if the API can't serve it whole.

    The API answers a whole-goal request with an HTTP 500 once the response would exceed
    its own buffer (as of the 2026 Q2 release this is the case for goal 4 — it fails for
    every area-code combination that is large enough, deterministically, so retrying the
    same request never succeeds). Asking for the areas in batches keeps every response
    under that limit; the union of the batches is the same set of areas, so the data is
    unchanged. Goals that still fit are fetched in a single request as before.
    """
    try:
        return [_read_goal_csv(_download_with_retries(url, goal, area_codes, attempts=WHOLE_GOAL_ATTEMPTS))]
    except requests.exceptions.HTTPError as e:
        log.warning(
            "Whole-goal request failed; splitting into area batches.",
            goal=goal,
            chunk_size=AREA_CHUNK_SIZE,
            error=str(e),
        )

    frames = []
    for i in range(0, len(area_codes), AREA_CHUNK_SIZE):
        chunk = area_codes[i : i + AREA_CHUNK_SIZE]
        df = _read_goal_csv(_download_with_retries(url, goal, chunk, attempts=MAX_RETRIES))
        log.info("Downloaded area batch", goal=goal, areas=f"{i}-{i + len(chunk)}", rows=len(df))
        # Batches covering only aggregates/unused areas legitimately come back empty; keep
        # only the ones with data so they don't muddy the concatenated dtypes.
        if len(df) > 0:
            frames.append(df)
    return frames


def _download_with_retries(url: str, goal: str, area_codes: list, attempts: int) -> bytes:
    """Run one download, retrying on transient network/server failures."""
    for attempt in range(1, attempts + 1):
        try:
            return download_file(url=url, goal=goal, area_codes=area_codes, max_retries=MAX_RETRIES)
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            if attempt == attempts:
                raise
            wait = min(RETRY_BACKOFF_SECONDS * 2 ** (attempt - 1), MAX_RETRY_WAIT_SECONDS)
            log.warning("Connection failed, retrying...", goal=goal, attempt=attempt, wait_seconds=wait, error=str(e))
            time.sleep(wait)
        except requests.exceptions.HTTPError as e:
            # A 5xx on this endpoint is usually a response-size limit rather than a blip, so
            # retry only briefly before letting the caller fall back to smaller requests.
            if attempt == attempts:
                raise
            log.warning(
                "Request failed, retrying...",
                goal=goal,
                attempt=attempt,
                wait_seconds=RETRY_BACKOFF_SECONDS,
                error=str(e),
            )
            time.sleep(RETRY_BACKOFF_SECONDS)
    # Unreachable: the loop either returns or raises.
    raise AssertionError(f"Failed to download goal {goal}.")


def _read_goal_csv(content: bytes) -> pd.DataFrame:
    """Parse one CSV response.

    NOTE: the API pads every response with NUL bytes up to a power-of-two buffer size.
    They are deliberately left in place: pandas turns the padding into a single all-NaN
    row, which makes the numeric columns floats, and therefore stores e.g. `year` as
    "2011.0" and `goal` as "4.0" once everything is cast to string below. Those columns
    are part of the garden index, so stripping the padding here would rewrite every
    index value in the garden dataset. The junk rows are dropped in the meadow step
    (`dropna(subset=["Value"])` plus the numeric filter). Cleaning this up is worth doing
    on its own, where the resulting whole-dataset diff is expected — see
    https://github.com/owid/etl/issues/447 for the related long-format cleanup.
    """
    # Guard against a genuinely truncated response: real content always ends with a
    # complete CSV line before the padding starts.
    assert content.rstrip(b"\x00").endswith(b"\n"), "CSV response ended mid-line — download was truncated."
    return pd.read_csv(StringIO(str(content, "utf-8")), low_memory=False)


def download_file(url: str, goal: str, area_codes: list, max_retries: int, bytes_read: int = 0) -> bytes:
    """Downloads a file from a url.

    Retries download up to {max_retries} times following a ChunkedEncodingError
    exception.
    """
    log.info(
        "Downloading data...",
        url=url,
        bytes_read=bytes_read,
        remaining_retries=max_retries,
        goal=goal,
    )
    if bytes_read:
        headers = {"Range": f"bytes={bytes_read}-"}
    else:
        headers = {}

    content = b""
    try:
        with requests.post(
            url,
            data={"goal": goal, "areaCodes": area_codes},
            headers=headers,
            stream=True,
        ) as r:
            r.raise_for_status()
            for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                bytes_read += CHUNK_SIZE
                content += chunk
    except requests.exceptions.ChunkedEncodingError:
        if max_retries > 0:
            log.info("Encountered ChunkedEncodingError, resuming download...")
            content += download_file(
                url=url,
                goal=goal,
                area_codes=area_codes,
                max_retries=max_retries - 1,
                bytes_read=bytes_read,
            )
        else:
            # Returning the partial content here would silently publish a truncated
            # snapshot, so fail loudly instead.
            raise RuntimeError(
                f"Encountered ChunkedEncodingError for goal {goal}, but max_retries has been "
                f"exceeded after reading {bytes_read} bytes. Download was not completed."
            )
    return content


def attributes_description(snap: Snapshot) -> dict[Any, Any]:
    """Gathers each of the unit codes and their more descriptive counterparts."""
    goal_codes = get_goal_codes(snap)
    a = []
    for goal in goal_codes:
        url = f"{SDG_DATA_API}/v1/sdg/Goal/{goal}/Attributes"
        res = requests.get(url)
        assert res.ok
        attr = res.json()
        for att in attr:
            for code in att["codes"]:
                a.append(
                    {
                        "code": code["code"],
                        "description": code["description"],
                    }
                )
    att_dict = pd.DataFrame(a).drop_duplicates().set_index("code").squeeze().to_dict()
    att_dict["PERCENT"] = "%"
    return att_dict


def dimensions_description(snap: Snapshot) -> dict:
    """Gathers each of the dimension codes and their more descriptive versions. This updates regularly so is important to snapshot"""
    goal_codes = get_goal_codes(snap)
    d = []
    for goal in goal_codes:
        url = f"{SDG_DATA_API}/v1/sdg/Goal/{goal}/Dimensions"
        res = requests.get(url)
        assert res.ok
        dims = res.json()
        for dim in dims:
            for code in dim["codes"]:
                d.append(
                    {
                        "id": dim["id"],
                        "code": code["code"],
                        "description": code["description"],
                    }
                )
    dim_dict = defaultdict(lambda: {np.nan: ""})
    for dimen in d:
        dim_dict[dimen["id"]][dimen["code"]] = dimen["description"]

    return dim_dict


def get_goal_codes(snap: Snapshot) -> list[int]:
    # retrieves all goal codes
    url = f"{SDG_DATA_API}/v1/sdg/Goal/List"
    res = requests.get(url)
    assert res.ok
    goals = res.json()
    goal_codes = [int(goal["code"]) for goal in goals]
    return goal_codes
