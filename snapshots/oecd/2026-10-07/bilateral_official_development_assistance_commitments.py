"""Script to create a snapshot of dataset.

Total bilateral ODA commitments from the OECD Creditor Reporting System (CRS), in constant prices: by donor (to all
developing countries) and by recipient (from all DAC members), for all sectors and all co-operation modalities. Unlike
the RioMarkers dataflow, it includes the activities that are outside the scope of the Rio markers (e.g. general budget
support, administrative costs and refugees in donor countries).
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

# The dataflow is hosted externally, so the API serves it from the dcd-public endpoint.
# NOTE: The OECD publishes new versions of the dataflow without notice, and a new version can be visible while it is
# still being loaded (in October 2026, version 1.7 was missing several years). So the version is pinned to a complete
# release, and the script warns when a newer one exists. At every update, check the newest version before moving to it,
# and keep it on the same release as the RioMarkers snapshot (climate_related_official_development_assistance).
BASE_URL = "https://sdmx.oecd.org/dcd-public/rest"
AGENCY = "OECD.DCD.FSD"
DATAFLOW_ID = "DSD_CRS@DF_CRS"
DATAFLOW_VERSION = "1.6"

# Keys follow the dimension order DONOR.RECIPIENT.SECTOR.MEASURE.CHANNEL.MODALITY.FLOW_TYPE.PRICE_BASE.MD_DIM.MD_ID.UNIT_MEASURE
# ODA (100), all sectors (1000), all channels and modalities (_T), commitments (C), constant prices (Q), aggregates (_T).
KEYS = {
    # All donors, to all developing countries (DPGC).
    "by donor": ".DPGC.1000.100._T._T.C.Q._T..USD",
    # All DAC members (DAC_EC), to each recipient and group of recipients.
    "by recipient": "DAC_EC..1000.100._T._T.C.Q._T..USD",
}

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
    url_data = f"{BASE_URL}/data/{AGENCY},{DATAFLOW_ID},{DATAFLOW_VERSION}"

    # Fetch both queries and concatenate them under a single header.
    header = None
    lines = []
    for name, key in KEYS.items():
        log.info(f"Downloading totals {name}.")
        content = fetch(
            url=f"{url_data}/{key}", params={"dimensionAtObservation": "AllDimensions", "format": "csvfile"}
        )
        query_header, _, rows = content.partition(b"\n")
        if header is None:
            header = query_header
            lines.append(header + b"\n")
        assert query_header == header, f"Unexpected change in columns of totals {name}."
        lines.append(rows if rows.endswith(b"\n") else rows + b"\n")

    with tempfile.TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / snap.path.name
        path.write_bytes(b"".join(lines))

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
