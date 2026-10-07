"""Script to create a snapshot of dataset.

The snapshot is a zip with files from the OECD RioMarkers dataflow (all amounts in constant prices) and its structure:
* totals_by_donor.csv: Bilateral allocable ODA commitments by donor (to all developing countries), for every score of
  every Rio marker. Summing all scores of one marker gives total bilateral allocable ODA.
* totals_by_recipient.csv: The same, received by each recipient or group of recipients from all DAC members.
* totals_by_sector.csv: The same by donor and sector, for the climate change mitigation and adaptation markers.
* activities.csv: Project-level rows of the activities marked as targeting climate change mitigation or adaptation, as a
  principal or significant objective. Each row carries the scores of all markers, which is what makes it possible to
  separate mitigation-only, adaptation-only and overlapping activities.
* structure.xml: Structure of the dataflow, including the names of donors, recipients and sectors, and the sector
  hierarchy.
* recipient_groups.xml and donor_groups.xml: Hierarchies that define the members of each group of recipients
  (regions, income groups, least developed countries, etc.) and donors (DAC members, G7, etc.).
"""

import io
import tempfile
import time
import zipfile
from pathlib import Path

import pandas as pd
import requests
from structlog import get_logger

from etl.helpers import PathFinder

log = get_logger()

paths = PathFinder(__file__)

# The dataflow is hosted externally, so the API serves it from the dcd-public endpoint (the public one returns errors).
BASE_URL = "https://sdmx.oecd.org/dcd-public/rest"
DATAFLOW = "OECD.DCD.FSD,DSD_RIOMRKR@DF_RIOMARKERS,1.6"
URL_STRUCTURE = f"{BASE_URL}/dataflow/OECD.DCD.FSD/DSD_RIOMRKR@DF_RIOMARKERS/1.6?references=all"
URL_RECIPIENT_GROUPS = f"{BASE_URL}/hierarchicalcodelist/OECD.DCD.FSD/HCL_DACRECIPIENTS/1.5"
URL_DONOR_GROUPS = f"{BASE_URL}/hierarchicalcodelist/OECD.DCD.FSD/HCL_DACDONORS/1.6"

# Keys follow the dimension order DONOR.RECIPIENT.SECTOR.MEASURE.ALLOCABLE.MARKER.SCORE.FLOW_TYPE.PRICE_BASE.MD_DIM.MD_ID.UNIT_MEASURE
# Markers: 10 (biodiversity), 20 (climate change mitigation), 30 (climate change adaptation), 40 (desertification),
# 50 (environment).
# Scores: 2 (principal), 1 (significant), 0 (screened, not targeted), 99 (not screened).
# Totals of all sectors (1000), all scores and all markers, by donor (to all developing countries, DPGC) and by recipient
# (from all DAC members, DAC_EC).
KEY_TOTALS_BY_DONOR = ".DPGC.1000.100.2...C.Q._T..USD"
KEY_TOTALS_BY_RECIPIENT = "DAC_EC..1000.100.2...C.Q._T..USD"
# Totals by donor and sector, for the climate markers. Sectors are the DAC sectors (three-digit codes) and their groups.
SECTORS = [
    "1000", "450", "100", "110", "120", "130", "140", "150", "160", "200", "210", "220", "230", "240", "250", "300",
    "310", "320", "331", "332", "400", "410", "430", "500", "520", "530", "600", "700", "720", "730", "740", "998",
]  # fmt: skip
KEY_TOTALS_BY_SECTOR = f".DPGC.{'+'.join(SECTORS)}.100.2.20+30..C.Q._T..USD"
# Activities: all sectors at project level (DD), climate markers, principal and significant scores only.
KEY_ACTIVITIES = "...100.2.20+30.1+2.C.Q.DD..USD"

# The API allows only a few dozen downloads per hour, so activities are fetched in blocks of years (recent years are
# ~50 MB each), and rate-limited requests are retried after a long wait.
YEARS_PER_REQUEST = 3
TIMEOUT_SECONDS = 900
MAX_ATTEMPTS = 4
WAIT_AFTER_RATE_LIMIT_SECONDS = 15 * 60


def run(upload: bool = True) -> None:
    """Create a new snapshot.

    Args:
        upload: Whether to upload the snapshot to S3.
    """
    # Init Snapshot object
    snap = paths.init_snapshot()

    with tempfile.TemporaryDirectory() as temp_dir:
        path_zip = Path(temp_dir) / snap.path.name
        with zipfile.ZipFile(path_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            # Structure and hierarchies of donors and recipients.
            for name, url in [
                ("structure.xml", URL_STRUCTURE),
                ("recipient_groups.xml", URL_RECIPIENT_GROUPS),
                ("donor_groups.xml", URL_DONOR_GROUPS),
            ]:
                log.info(f"Downloading {name}.")
                zf.writestr(name, fetch(url=url, params={}))

            # Totals, for all years at once.
            for name, key in [
                ("totals_by_donor.csv", KEY_TOTALS_BY_DONOR),
                ("totals_by_recipient.csv", KEY_TOTALS_BY_RECIPIENT),
                ("totals_by_sector.csv", KEY_TOTALS_BY_SECTOR),
            ]:
                log.info(f"Downloading {name}.")
                zf.writestr(name, fetch_data(key=key, params={}))

            # Years covered by the totals define the years to fetch at project level.
            years = sorted(_years_in_csv(zf.read("totals_by_donor.csv")))
            log.info(f"Downloading activities for {years[0]}-{years[-1]}.")

            # Activities, in blocks of years, concatenated under a single header.
            header = None
            with zf.open("activities.csv", "w") as f:
                for start in range(years[0], years[-1] + 1, YEARS_PER_REQUEST):
                    end = min(start + YEARS_PER_REQUEST - 1, years[-1])
                    content = fetch_data(key=KEY_ACTIVITIES, params={"startPeriod": start, "endPeriod": end})
                    block_header, _, rows = content.partition(b"\n")
                    if header is None:
                        header = block_header
                        f.write(header + b"\n")
                    assert block_header == header, f"Unexpected change in columns of activities for {start}-{end}."
                    f.write(rows if rows.endswith(b"\n") else rows + b"\n")
                    log.info(f"Downloaded activities for {start}-{end} ({len(content) / 1e6:.1f} MB).")

        # Save snapshot.
        snap.create_snapshot(filename=path_zip, upload=upload)


def fetch_data(key: str, params: dict) -> bytes:
    """Fetch a data query from the OECD SDMX API as a flat CSV."""
    params = {"dimensionAtObservation": "AllDimensions", "format": "csvfile", **params}
    return fetch(url=f"{BASE_URL}/data/{DATAFLOW}/{key}", params=params)


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


def _years_in_csv(content: bytes) -> set[int]:
    """Years present in a CSV downloaded from the API."""
    return set(pd.read_csv(io.BytesIO(content), usecols=["TIME_PERIOD"])["TIME_PERIOD"].astype(int))
