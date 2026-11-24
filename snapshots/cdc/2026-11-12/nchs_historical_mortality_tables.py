"""Snapshot of NCHS's historical mortality tables HIST290 (death rates) and HIST290A (deaths), 1900–1998.

NCHS publishes each table as one PDF per period of the International Classification of Diseases (ICD): 1900–39,
1940–49, 1950–59, 1960–67, 1968–78 and 1979–98. This script downloads all twelve PDFs unchanged and bundles them
into a single zip archive.

NOTE: The 1960–67 deaths table (dx196067.pdf) returns 404 at the address linked from the HIST290A page on
cdc.gov, but is still served from CDC's archive site, which is used below. On the next update, check whether the
cdc.gov address works again.
"""

import tempfile
import zipfile
from pathlib import Path

import requests

from etl.helpers import PathFinder

paths = PathFinder(__file__)

BASE_URL = "https://www.cdc.gov/nchs/data"

# File name inside the archive -> download URL.
FILES = {
    # HIST290: death rates for selected causes, by 10-year age groups, race and sex.
    "hist290_death_rates_1900_1939.pdf": f"{BASE_URL}/dvs/hist290_0039.pdf",
    "hist290_death_rates_1940_1949.pdf": f"{BASE_URL}/dvs/mx194049.pdf",
    "hist290_death_rates_1950_1959.pdf": f"{BASE_URL}/dvs/mx1950_59.pdf",
    "hist290_death_rates_1960_1967.pdf": f"{BASE_URL}/dvs/mx196067.pdf",
    "hist290_death_rates_1968_1978.pdf": f"{BASE_URL}/dvs/mx196878.pdf",
    "hist290_death_rates_1979_1998.pdf": f"{BASE_URL}/statab/gm290-98.pdf",
    # HIST290A: number of deaths for the same causes.
    "hist290a_deaths_1900_1939.pdf": f"{BASE_URL}/dvs/hist290a_0039.pdf",
    "hist290a_deaths_1940_1949.pdf": f"{BASE_URL}/dvs/dx194049.pdf",
    "hist290a_deaths_1950_1959.pdf": f"{BASE_URL}/dvs/dx1950_59.pdf",
    "hist290a_deaths_1960_1967.pdf": "https://archive.cdc.gov/www_cdc_gov/nchs/data/statab/dx196067.pdf",
    "hist290a_deaths_1968_1978.pdf": f"{BASE_URL}/dvs/dx196878.pdf",
    "hist290a_deaths_1979_1998.pdf": f"{BASE_URL}/statab/gmwk290a.pdf",
}


def run(upload: bool = True) -> None:
    """Create a new snapshot.

    Args:
        upload: Whether to upload the snapshot to S3.
    """
    snap = paths.init_snapshot()

    with tempfile.TemporaryDirectory() as tmp:
        archive_path = Path(tmp) / "nchs_historical_mortality_tables.zip"
        with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for name, url in FILES.items():
                # NOTE: cdc.gov rejects browser-like user agents with 403, so keep the default one from requests.
                response = requests.get(url, timeout=300)
                response.raise_for_status()
                assert response.content[:4] == b"%PDF", f"{url} did not return a PDF."
                archive.writestr(name, response.content)

        snap.create_snapshot(filename=archive_path, upload=upload)
