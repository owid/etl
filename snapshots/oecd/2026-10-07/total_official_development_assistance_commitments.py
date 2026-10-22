"""Script to create a snapshot of dataset.

Total ODA commitments from the OECD DAC1 table, in constant prices, for DAC countries (as a group) and EU institutions:
their total ODA, their bilateral ODA, and the DAC countries' contributions to EU institutions. Together with the
bilateral ODA in the CRS, they give the share of all development aid that is bilateral, counting EU institutions as a
donor without counting the DAC countries' contributions to them twice.
"""

import re
import tempfile
import time
from pathlib import Path

import requests
from structlog import get_logger

from etl.helpers import PathFinder

log = get_logger()

paths = PathFinder(__file__)

# NOTE: The OECD publishes new versions of the dataflow without notice. The version is pinned so the data does not
# change silently, and the script warns when a newer one exists. At every update, check the newest version (complete
# years, same totals as the Data Explorer) before moving to it.
BASE_URL = "https://sdmx.oecd.org/public/rest"
# Publisher of the dataflow: the OECD's Development Co-operation Directorate (DCD), Financing for Sustainable Development
# division (FSD).
AGENCY = "OECD.DCD.FSD"
# Dataflow (dataset) identifier: "<data structure>@<dataflow>".
DATAFLOW_ID = "DSD_DAC1@DF_DAC1"
# Version of the dataflow: a release of the dataset (its structure and data).
DATAFLOW_VERSION = "1.8"

# Data query, with one or more codes for each dimension, separated by dots (and joined by "+"):
#   1. DONOR: DAC (DAC countries, as a group) and 4EU001 (EU institutions).
#   2. SECTOR: _Z (not applicable).
#   3. MEASURE: 1010 (total ODA), 1015 (bilateral ODA), 2102 (contributions to EU institutions).
#   4. TYING_STATUS: _Z (not applicable).
#   5. FLOW_TYPE: 1150 (commitments, total).
#   6. UNIT_MEASURE: USD (US dollars, in millions).
#   7. PRICE_BASE: Q (constant prices, of the base year in the data).
KEY = "DAC+4EU001._Z.1010+1015+2102._Z.1150.USD.Q"

TIMEOUT_SECONDS = 600
MAX_ATTEMPTS = 4
WAIT_AFTER_RATE_LIMIT_SECONDS = 15 * 60


def run(upload: bool = True) -> None:
    """Create a new snapshot.

    Args:
        upload: Whether to upload the snapshot to S3.
    """
    # Init Snapshot object
    snap = paths.init_snapshot()

    # Warn when the OECD has published a version newer than the pinned one.
    latest = fetch(url=f"{BASE_URL}/dataflow/{AGENCY}/{DATAFLOW_ID}/latest", params={}).decode("utf-8")
    match = re.search(r'<structure:Dataflow [^>]*version="([^"]+)"', latest)
    assert match, "Dataflow not found in the structure."
    log.info(f"Using dataflow version {DATAFLOW_VERSION}; the latest is {match.group(1)}.")
    if match.group(1) != DATAFLOW_VERSION:
        log.warning(f"Dataflow version {match.group(1)} exists. Check that it is complete before moving to it.")

    content = fetch(
        url=f"{BASE_URL}/data/{AGENCY},{DATAFLOW_ID},{DATAFLOW_VERSION}/{KEY}",
        params={"dimensionAtObservation": "AllDimensions", "format": "csvfile"},
    )

    with tempfile.TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / snap.path.name
        path.write_bytes(content)

        # Save snapshot.
        snap.create_snapshot(filename=path, upload=upload)


def fetch(url: str, params: dict) -> bytes:
    """Fetch a query from the OECD SDMX API, retrying on rate limits and server errors."""
    for attempt in range(1, MAX_ATTEMPTS + 1):
        response = requests.get(url, params=params, timeout=TIMEOUT_SECONDS)
        if (response.status_code != 429 and response.status_code < 500) or attempt == MAX_ATTEMPTS:
            break
        wait = WAIT_AFTER_RATE_LIMIT_SECONDS if response.status_code == 429 else 30 * attempt
        log.warning(f"Error {response.status_code} (attempt {attempt}); retrying in {wait} seconds.")
        time.sleep(wait)
    response.raise_for_status()
    return response.content
