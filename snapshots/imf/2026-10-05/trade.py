"""Script to create a snapshot of dataset.

The data comes from the IMF's public SDMX 3.0 API. Each indicator is fetched in its own request (annual series for all
reporting and partner countries), and every response is stored unchanged as one CSV inside a zip archive.

The API identifies countries and indicators by their codes (e.g. "USA", "G001" for World, "XG_FOB_USD"), and reports
values in US dollars (the SCALE attribute is only a display hint).
"""

import tempfile
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Annual series of a given indicator, for all reporting and partner countries.
API_URL = "https://api.imf.org/external/sdmx/3.0/data/dataflow/IMF.STA/IMTS/+/*.{indicator}.*.A"
HEADERS = {"Accept": "application/vnd.sdmx.data+csv;version=2.0.0"}

# Indicator codes to fetch.
INDICATORS = [
    # Exports of goods, Free on board (FOB), US dollar.
    "XG_FOB_USD",
    # Imports of goods, Cost insurance freight (CIF), US dollar.
    "MG_CIF_USD",
    # Imports of goods, Free on board (FOB), US dollar.
    "MG_FOB_USD",
    # Trade balance goods, US dollar.
    "TBG_USD",
]

# Each request takes several minutes on the IMF side.
TIMEOUT = (30, 1800)
MAX_ATTEMPTS = 3


def run(upload: bool = True) -> None:
    # Initialize a new snapshot.
    snap = Snapshot(f"imf/{SNAPSHOT_VERSION}/trade.zip")

    # Fetch all indicators in parallel.
    with ThreadPoolExecutor(max_workers=len(INDICATORS)) as executor:
        responses = dict(zip(INDICATORS, executor.map(fetch_indicator, INDICATORS)))

    with tempfile.TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "trade.zip"
        with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for indicator, content in responses.items():
                archive.writestr(f"{indicator}.csv", content)

        # Save snapshot.
        snap.create_snapshot(filename=path, upload=upload)


def fetch_indicator(indicator: str) -> bytes:
    url = API_URL.format(indicator=indicator)
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            response = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            response.raise_for_status()
            break
        except requests.RequestException:
            if attempt == MAX_ATTEMPTS:
                raise
            time.sleep(30 * attempt)

    # Sanity check: the response is an SDMX-CSV file with observations.
    header = response.content.split(b"\n", 1)[0].decode()
    assert {"COUNTRY", "INDICATOR", "COUNTERPART_COUNTRY", "TIME_PERIOD", "OBS_VALUE"} <= set(header.split(",")), (
        f"Unexpected header for {indicator}: {header}"
    )
    assert response.content.count(b"\n") > 10_000, f"Suspiciously few rows for {indicator}."

    return response.content
