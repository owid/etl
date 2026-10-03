"""Script to create a snapshot of the UNSD Energy Balances.

Data is downloaded from the UNdata SDMX REST API.
API documentation: https://data.un.org/Host.aspx?Content=API

The dataflow is DF_UNData_EnergyBalance (UNSD, v1.0) with dimensions:
  1. REF_AREA - Country/area code
  2. COMMODITY - Energy commodity code
  3. TRANSACTION - Energy flow (transaction) code
  4. UNIT - Unit of measurement code
  5. TIME_PERIOD - Year

The snapshot stores the raw API responses in a zip archive:
  - energy_balances.xml: full data in SDMX-ML GenericData format.
  - structure.xml: dataflow structure, including the codelists that map dimension codes to human-readable names
    (CL_AREA, CL_COMMODITY_ENERGY_BALANCE_UNDATA, CL_TRANS_ENERGY_BALANCE_UNDATA, CL_UNIT_ENERGY_UNDATA).
"""

import tempfile
import zipfile
from pathlib import Path

import click
import requests
from structlog import get_logger

from etl.snapshot import Snapshot

log = get_logger()

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# UNdata SDMX REST API base URL.
API_BASE_URL = "https://data.un.org/ws/rest"

# Dataflow for the Energy Balances.
DATAFLOW_ID = "DF_UNData_EnergyBalance"

# Download all data (all areas, commodities, transactions, units and years).
DATA_URL = f"{API_BASE_URL}/data/{DATAFLOW_ID}/all"

# Download the dataflow structure with all referenced codelists in a single call.
STRUCTURE_URL = f"{API_BASE_URL}/dataflow/all/{DATAFLOW_ID}/latest?references=all"

# Timeout for the API request (the full dataset is ~270 MB and takes ~2 minutes).
REQUEST_TIMEOUT = 900


def download(url: str, timeout: int = REQUEST_TIMEOUT) -> bytes:
    """Download a file from the UNdata SDMX API."""
    log.info("download.start", url=url)

    response = requests.get(url=url, timeout=timeout)
    response.raise_for_status()

    size_mb = len(response.content) / (1024 * 1024)
    log.info("download.done", size_mb=f"{size_mb:.1f}")

    return response.content


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def run(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"un/{SNAPSHOT_VERSION}/energy_balances.zip")

    # Download all energy balances data and the dataflow structure (which includes all codelists).
    data = download(DATA_URL)
    structure = download(STRUCTURE_URL)

    # Save both raw XML files as a compressed ZIP file.
    with tempfile.TemporaryDirectory() as temp_dir:
        output_file = Path(temp_dir) / "energy_balances.zip"
        with zipfile.ZipFile(output_file, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("energy_balances.xml", data)
            zf.writestr("structure.xml", structure)

        # Add file to DVC and upload to S3.
        snap.create_snapshot(filename=output_file, upload=upload)
