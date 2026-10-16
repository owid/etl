"""Script to create a snapshot of dataset.

The snapshot is a zip with one file from the OECD RioMarkers dataflow (amounts in constant prices):
* not_screened_activities.csv: Project-level rows of the activities that donors did not screen against the climate change
  mitigation marker (score 99), from 2010 onwards. Each row carries the scores of all markers, which is what makes it
  possible to find the activities not screened for adaptation either.
"""

import tempfile
import time
import zipfile
from datetime import date
from pathlib import Path

import requests
from structlog import get_logger

from etl.helpers import PathFinder

log = get_logger()

paths = PathFinder(__file__)

# NOTE: Keep the same dataflow version as the main RioMarkers snapshot (climate_related_official_development_assistance).
BASE_URL = "https://sdmx.oecd.org/dcd-public/rest"
AGENCY = "OECD.DCD.FSD"
DATAFLOW_ID = "DSD_RIOMRKR@DF_RIOMARKERS"
DATAFLOW_VERSION = "1.6"
# Project-level rows ("DD") of ODA, bilateral allocable, mitigation marker (20), not screened (99), commitments, constant
# prices, in US dollars (see the main RioMarkers snapshot script for all dimensions and codes).
KEY_NOT_SCREENED = "...100.2.20.99.C.Q.DD..USD"
# The adaptation marker applies from 2010, the first year of the garden data.
START_YEAR = 2010

# The API allows only a few dozen downloads per hour, so rate-limited requests are retried after a long wait.
TIMEOUT_SECONDS = 900
MAX_ATTEMPTS = 4
WAIT_AFTER_RATE_LIMIT_SECONDS = 15 * 60


def run(upload: bool = True) -> None:
    """Create a new snapshot.

    Args:
        upload: Whether to upload the snapshot to S3.
    """
    snap = paths.init_snapshot()

    with tempfile.TemporaryDirectory() as temp_dir:
        path_zip = Path(temp_dir) / snap.path.name
        with zipfile.ZipFile(path_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            # One year per request, concatenated under a single header (years without data are skipped).
            header = None
            with zf.open("not_screened_activities.csv", "w") as f:
                for year in range(START_YEAR, date.today().year):
                    content = fetch_year(year=year)
                    if content is None:
                        log.info(f"No activities for {year}.")
                        continue
                    block_header, _, rows = content.partition(b"\n")
                    if header is None:
                        header = block_header
                        f.write(header + b"\n")
                    assert block_header == header, f"Unexpected change in columns of activities for {year}."
                    f.write(rows if rows.endswith(b"\n") else rows + b"\n")
                    log.info(f"Downloaded activities for {year} ({len(content) / 1e6:.1f} MB).")
            assert header is not None, "No activities downloaded."

        snap.create_snapshot(filename=path_zip, upload=upload)


def fetch_year(year: int) -> bytes | None:
    """Fetch the activities of one year as a flat CSV, or None if the dataflow has no data for that year."""
    url = f"{BASE_URL}/data/{AGENCY},{DATAFLOW_ID},{DATAFLOW_VERSION}/{KEY_NOT_SCREENED}"
    params = {"dimensionAtObservation": "AllDimensions", "format": "csvfile", "startPeriod": year, "endPeriod": year}
    for attempt in range(1, MAX_ATTEMPTS + 1):
        response = requests.get(url, params=params, timeout=TIMEOUT_SECONDS)
        if (response.status_code != 429 and response.status_code < 500) or attempt == MAX_ATTEMPTS:
            break
        wait = WAIT_AFTER_RATE_LIMIT_SECONDS if response.status_code == 429 else 30 * attempt
        log.warning(f"Error {response.status_code} (attempt {attempt}); retrying in {wait} seconds.")
        time.sleep(wait)
    # The API answers 404 when a query has no data.
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.content
