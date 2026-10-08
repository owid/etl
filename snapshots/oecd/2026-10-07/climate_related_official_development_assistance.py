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
import re
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
# NOTE: The OECD publishes new versions of the dataflow without notice, and a new version can be visible while it is
# still being loaded (in October 2026, version 1.7 had a partial 2025 and, for the CRS, several years missing). So the
# dataflow version is pinned to a complete release, and the script warns when a newer one exists. At every update,
# check the newest version (complete years, same totals as the Data Explorer) before moving to it, and keep the CRS
# snapshot (bilateral_official_development_assistance_commitments) on the same release.
# The hierarchies of donors and recipients are always the latest versions, which have the latest income classification
# of the World Bank. The garden step computes the totals of groups of recipients from their members, so they use the
# same classification even when the dataflow links to an older hierarchy (version 1.6 links to recipients version 1.5).
BASE_URL = "https://sdmx.oecd.org/dcd-public/rest"
# Publisher of the dataflow: the OECD's Development Co-operation Directorate (DCD), Financing for Sustainable Development
# division (FSD).
AGENCY = "OECD.DCD.FSD"
# Dataflow (dataset) identifier: "<data structure>@<dataflow>".
DATAFLOW_ID = "DSD_RIOMRKR@DF_RIOMARKERS"
# Version of the dataflow: a release of the dataset (its structure and data).
DATAFLOW_VERSION = "1.6"
# Hierarchical codelists, which define the members of each group of recipients (regions, income groups, least developed
# countries...) and of donors (DAC countries, G7...). They have their own versions.
HIERARCHIES = {"recipient_groups.xml": "HCL_DACRECIPIENTS", "donor_groups.xml": "HCL_DACDONORS"}

# Data queries select one or more codes for each dimension of the dataflow, separated by dots, in this order (an empty
# position means all codes, and "+" joins several codes):
#   1. DONOR: a donor code, e.g. GBR, 4EU001 (EU institutions), or DAC_EC (all DAC members, i.e. DAC countries and EU
#      institutions).
#   2. RECIPIENT: a recipient code, e.g. IND, F6 (South of Sahara), LDC (least developed countries), DPGC (all
#      developing countries), or DPGC_X (developing countries, unspecified: aid not assigned to any country or region).
#   3. SECTOR: a DAC sector code, e.g. 230 (energy); 1000 is all sectors.
#   4. MEASURE: 100 is official development assistance (ODA), the only option.
#   5. ALLOCABLE: 2 is bilateral allocable aid (aid assigned to a recipient and a sector, which is what the Rio markers
#      apply to), the only option.
#   6. MARKER: 10 (biodiversity), 20 (climate change mitigation), 30 (climate change adaptation), 40 (desertification),
#      50 (environment).
#   7. SCORE: 2 (principal objective), 1 (significant objective), 0 (screened, not targeted), 99 (not screened).
#   8. FLOW_TYPE: C is commitments, the only option.
#   9. PRICE_BASE: Q is constant prices (of the base year in the data, 2024 in this release); V is current prices.
#  10. MD_DIM: _T is totals; DD is project-level rows ("drilldown").
#  11. MD_ID: the project identifier (left empty, i.e. all).
#  12. UNIT_MEASURE: USD (US dollars, in millions).
# Totals of all sectors, all markers and all scores, by donor (to all developing countries) and by recipient (from all DAC
# members).
KEY_TOTALS_BY_DONOR = ".DPGC.1000.100.2...C.Q._T..USD"
KEY_TOTALS_BY_RECIPIENT = "DAC_EC..1000.100.2...C.Q._T..USD"
# Totals by donor and sector, for the climate markers: the DAC sectors (three-digit codes) and their groups.
SECTORS = [
    "1000",  # All sectors
    "450",  # Sector allocable
    "100", "110", "120", "130", "140", "150", "160",  # Social infrastructure: education, health, population, water...
    "200", "210", "220", "230", "240", "250",  # Economic infrastructure: transport, communications, energy, banking...
    "300", "310", "320", "331", "332",  # Production: agriculture, industry, trade, tourism
    "400", "410", "430",  # Multi-sector: general environment protection, other multisector
    "500", "520", "530",  # Commodity aid: development food assistance, other commodity assistance
    "600",  # Action relating to debt
    "700", "720", "730", "740",  # Humanitarian aid: emergency response, reconstruction, disaster prevention
    "998",  # Unallocated / unspecified
]  # fmt: skip
KEY_TOTALS_BY_SECTOR = f".DPGC.{'+'.join(SECTORS)}.100.2.20+30..C.Q._T..USD"
# Project-level rows of all donors, recipients and sectors, for the climate markers, principal and significant only.
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
            # Structure of the dataflow, and the latest hierarchies of donors and recipients.
            warn_if_newer_version()
            version = DATAFLOW_VERSION
            url = f"{BASE_URL}/dataflow/{AGENCY}/{DATAFLOW_ID}/{version}?references=all"
            zf.writestr("structure.xml", fetch(url=url, params={}))
            for name, hierarchy in HIERARCHIES.items():
                content = fetch(url=f"{BASE_URL}/hierarchicalcodelist/{AGENCY}/{hierarchy}/latest", params={})
                log.info(f"Using {hierarchy} version {get_version(content=content, element='HierarchicalCodelist')}.")
                zf.writestr(name, content)

            # Totals, for all years at once.
            for name, key in [
                ("totals_by_donor.csv", KEY_TOTALS_BY_DONOR),
                ("totals_by_recipient.csv", KEY_TOTALS_BY_RECIPIENT),
                ("totals_by_sector.csv", KEY_TOTALS_BY_SECTOR),
            ]:
                log.info(f"Downloading {name}.")
                zf.writestr(name, fetch_data(version=version, key=key, params={}))

            # Years covered by the totals define the years to fetch at project level.
            years = sorted(_years_in_csv(zf.read("totals_by_donor.csv")))
            log.info(f"Downloading activities for {years[0]}-{years[-1]}.")

            # Activities, in blocks of years, concatenated under a single header.
            header = None
            with zf.open("activities.csv", "w") as f:
                for start in range(years[0], years[-1] + 1, YEARS_PER_REQUEST):
                    end = min(start + YEARS_PER_REQUEST - 1, years[-1])
                    content = fetch_data(
                        version=version, key=KEY_ACTIVITIES, params={"startPeriod": start, "endPeriod": end}
                    )
                    block_header, _, rows = content.partition(b"\n")
                    if header is None:
                        header = block_header
                        f.write(header + b"\n")
                    assert block_header == header, f"Unexpected change in columns of activities for {start}-{end}."
                    f.write(rows if rows.endswith(b"\n") else rows + b"\n")
                    log.info(f"Downloaded activities for {start}-{end} ({len(content) / 1e6:.1f} MB).")

        # Save snapshot.
        snap.create_snapshot(filename=path_zip, upload=upload)


def warn_if_newer_version() -> None:
    """Warn when the OECD has published a version of the dataflow newer than the pinned one."""
    latest = get_version(content=fetch(url=f"{BASE_URL}/dataflow/{AGENCY}/{DATAFLOW_ID}/latest", params={}))
    log.info(f"Using dataflow version {DATAFLOW_VERSION}; the latest is {latest}.")
    if latest != DATAFLOW_VERSION:
        log.warning(
            f"Dataflow version {latest} exists. Check that it is complete before moving DATAFLOW_VERSION to it."
        )


def get_version(content: bytes, element: str = "Dataflow") -> str:
    """Version of the first element of a given type in an SDMX structure message."""
    match = re.search(rf'<structure:{element} [^>]*version="([^"]+)"', content.decode("utf-8"))
    assert match, f"No {element} found in the structure."
    return match.group(1)


def fetch_data(version: str, key: str, params: dict) -> bytes:
    """Fetch a data query from the OECD SDMX API as a flat CSV."""
    params = {"dimensionAtObservation": "AllDimensions", "format": "csvfile", **params}
    return fetch(url=f"{BASE_URL}/data/{AGENCY},{DATAFLOW_ID},{version}/{key}", params=params)


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
