"""Script to create a snapshot of dataset.

The data comes from the IMF's public SDMX 3.0 API. A single request fetches the annual series of the indicators we use,
for all countries and country groups, and the response is stored unchanged as a CSV file.

The API identifies countries and indicators by their codes (e.g. "USA", "G001" for World, "NGDP_RPCH" for real GDP
growth). Each row also carries the series attributes LATEST_ACTUAL_ANNUAL_DATA, the last year of actual (non-projected)
data, and START_END_MONTHS_OF_REPORTING_YEAR, the months a fiscal year runs (for countries that report on one); both
are used downstream to split observations from forecasts.

NOTE: The IMF republishes each WEO release under the same dataflow version (e.g. 9.0.0 held the April 2026 release and
then the October 2026 one), so the version wildcard "+" (latest) is safe here. Check PUBLICATION_DATE in the logged
output to confirm which release was fetched.
"""

import tempfile
import time
from pathlib import Path

import requests
from structlog import get_logger

from etl.snapshot import Snapshot

log = get_logger()

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Indicator codes to fetch.
INDICATORS = [
    # Gross domestic product (GDP), Constant prices, Percent change.
    "NGDP_RPCH",
    # Unemployment rate.
    "LUR",
]

# Annual series of the indicators above, for all countries, with the series attributes we need.
API_URL = (
    "https://api.imf.org/external/sdmx/3.0/data/dataflow/IMF.RES/WEO/+/*.{indicators}.A"
    "?attributes=SERIES_NAME,LATEST_ACTUAL_ANNUAL_DATA,START_END_MONTHS_OF_REPORTING_YEAR,PUBLICATION_DATE"
)
HEADERS = {"Accept": "application/vnd.sdmx.data+csv;version=2.0.0"}

TIMEOUT = (30, 600)
MAX_ATTEMPTS = 3


def run(upload: bool = True) -> None:
    # Initialize a new snapshot.
    snap = Snapshot(f"imf/{SNAPSHOT_VERSION}/world_economic_outlook.csv")

    # Fetch data from the API.
    content = fetch_data()

    # Save snapshot.
    with tempfile.TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "world_economic_outlook.csv"
        path.write_bytes(content)
        snap.create_snapshot(filename=path, upload=upload)


def fetch_data() -> bytes:
    url = API_URL.format(indicators="+".join(INDICATORS))
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            response = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            response.raise_for_status()
            break
        except requests.RequestException:
            if attempt == MAX_ATTEMPTS:
                raise
            time.sleep(30 * attempt)

    # Sanity checks: the response is an SDMX-CSV file with observations for every indicator.
    lines = response.content.decode().splitlines()
    header = lines[0].split(",")
    assert {"COUNTRY", "INDICATOR", "TIME_PERIOD", "OBS_VALUE", "LATEST_ACTUAL_ANNUAL_DATA"} <= set(header), (
        f"Unexpected header: {header}"
    )
    assert len(lines) > 10_000, "Suspiciously few rows."
    for indicator in INDICATORS:
        assert any(f",{indicator},A," in line for line in lines[1:]), f"No data for {indicator}."

    # Log the dataflow version and publication date, to confirm which WEO release was fetched.
    first_row = dict(zip(header, lines[1].split(",")))
    log.info("weo.fetched", structure=first_row.get("STRUCTURE_ID"), publication_date=first_row.get("PUBLICATION_DATE"))

    return response.content
