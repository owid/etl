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
        Snapshot(f"un/{SNAPSHOT_VERSION}/un_sdg_unit.csv").create_snapshot(data=unit_desc, upload=upload)

        log.info("Downloading dimension descriptions...")
        dim_desc = dimensions_description(snap)
        dim_file = os.path.join(temp_dir, "data.json")
        with open(dim_file, "w") as fp:
            json.dump(dim_desc, fp)

        log.info("Adding dimension descriptions to catalog...")
        Snapshot(f"un/{SNAPSHOT_VERSION}/un_sdg_dimension.json").create_snapshot(filename=dim_file, upload=upload)

        # fetch the file locally
        log.info("Downloading data...")
        all_data = download_data(snap)
        log.info("Adding data to catalog...")
        Snapshot(f"un/{SNAPSHOT_VERSION}/un_sdg.feather").create_snapshot(data=all_data, upload=upload)


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
        # Count real observations rather than parsed rows: the API's NUL padding becomes one
        # all-NaN row per response, so a header-only response still parses to a non-empty
        # frame. Without this, a goal that returned no data would pass unnoticed here and be
        # silently dropped in meadow.
        observations = sum(count_observations(f) for f in frames)
        assert observations > 0, f"No observations returned for goal {goal}."
        log.info("Downloaded goal", goal=goal, observations=observations, requests=len(frames))
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
        label = f"{i}-{i + len(chunk)}"
        df = _read_goal_csv(_download_with_retries(url, goal, chunk, attempts=MAX_RETRIES))
        observations = count_observations(df)

        if observations == 0:
            # A batch can legitimately carry no observations (areas this goal doesn't cover), but a
            # successful-but-empty response looks exactly the same while silently dropping up to
            # AREA_CHUNK_SIZE areas — and the goal-level assertion would still pass on the strength
            # of the other batches. The response alone can't tell the two apart, so emptiness is
            # only accepted once it reproduces on a second request.
            df = _read_goal_csv(_download_with_retries(url, goal, chunk, attempts=MAX_RETRIES))
            observations = count_observations(df)
            if observations == 0:
                log.warning(
                    "Area batch carries no observations; empty response confirmed on retry.",
                    goal=goal,
                    areas=label,
                    area_codes=chunk,
                )
                continue
            log.warning(
                "Area batch was empty on the first request but returned data on retry.",
                goal=goal,
                areas=label,
                observations=observations,
            )

        log.info("Downloaded area batch", goal=goal, areas=label, observations=observations)
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


def count_observations(df: pd.DataFrame) -> int:
    """Number of rows carrying an actual numeric value.

    `Value` is the field meadow filters on, so this is the count that decides whether a
    response really contributed data. It also excludes the all-NaN row the API's NUL padding
    produces (see `_read_goal_csv`).
    """
    return int(pd.to_numeric(df["Value"], errors="coerce").notna().sum())


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


def download_file(url: str, goal: str, area_codes: list, max_retries: int) -> bytes:
    """Download one goal's CSV, restarting from the beginning if the stream breaks.

    This endpoint advertises `Accept-Ranges: bytes` but ignores a `Range` header on POST: it answers
    200 with the complete body and no `Content-Range`. An earlier version of this function tried to
    resume after a `ChunkedEncodingError` by re-requesting with `Range` and appending the response to
    the bytes already read — which therefore glued a second, complete copy of the CSV onto a partial
    one. Nothing downstream could catch that: the result still ends in a complete line, so the
    truncation assert passes, and the duplicated rows survive into the snapshot. Restarting the
    download is the only correct option against a server that cannot resume.
    """
    for attempt in range(1, max_retries + 2):
        log.info("Downloading data...", url=url, goal=goal, attempt=attempt, max_attempts=max_retries + 1)
        content = b""
        try:
            with requests.post(url, data={"goal": goal, "areaCodes": area_codes}, stream=True) as r:
                r.raise_for_status()
                for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                    content += chunk
            return content
        except requests.exceptions.ChunkedEncodingError:
            if attempt > max_retries:
                raise RuntimeError(
                    f"Stream broke for goal {goal} and max_retries has been exceeded after reading "
                    f"{len(content)} bytes. Download was not completed."
                ) from None
            log.info(
                "Stream broke; restarting the download from the beginning.",
                goal=goal,
                bytes_discarded=len(content),
                attempt=attempt,
            )

    raise AssertionError("unreachable: the loop either returns or raises")


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
