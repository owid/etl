"""Script to create a snapshot of the UN Energy Statistics Database.

Data is downloaded from the UNdata SDMX REST API.
API documentation: https://data.un.org/Host.aspx?Content=API

The dataflow is DF_UNDATA_ENERGY (UNSD, v1.2) with dimensions:
  1. FREQ - Frequency (A = Annual)
  2. REF_AREA - Country/region code
  3. COMMODITY - Energy commodity code
  4. TRANSACTION - Transaction type code
  5. TIME_PERIOD - Year

Codelists map dimension codes to human-readable names:
  - CL_AREA_NRG: REF_AREA codes -> country/region names
  - CL_COMMODITY_NRG: COMMODITY codes -> energy commodity names
  - CL_TRANSACTION_NRG: TRANSACTION codes -> transaction type names
  - CL_UNIT_MEASURE_NRG: UNIT_MEASURE codes -> unit names

"""

import tempfile
import zipfile
from io import StringIO
from pathlib import Path
from xml.etree import ElementTree

import click
import pandas as pd
import requests
from structlog import get_logger

from etl.snapshot import Snapshot

log = get_logger()

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# UNdata SDMX REST API base URL.
API_BASE_URL = "https://data.un.org/ws/rest"

# Dataflow for the Energy Statistics Database.
DATAFLOW_ID = "DF_UNDATA_ENERGY"

# Download all annual data (all countries, commodities, transactions, years).
# Key format: FREQ.REF_AREA.COMMODITY.TRANSACTION
DATA_URL = f"{API_BASE_URL}/data/{DATAFLOW_ID}/A...."

# Codelists to fetch (maps dimension codes to human-readable names).
CODELISTS = {
    "REF_AREA": "UNSD/CL_AREA_NRG",
    "COMMODITY": "UNSD/CL_COMMODITY_NRG",
    "TRANSACTION": "UNSD/CL_TRANSACTION_NRG",
    "UNIT_MEASURE": "UNSD/CL_UNIT_MEASURE_NRG",
}

# Timeout for the API request (the full dataset is ~130 MB and takes ~1-2 minutes).
REQUEST_TIMEOUT = 600


def download_data(url: str = DATA_URL, timeout: int = REQUEST_TIMEOUT) -> bytes:
    """Download all energy statistics data from the UNdata SDMX API in CSV format."""
    log.info("download_data.start", url=url)

    response = requests.get(
        url=url,
        headers={"Accept": "text/csv"},
        timeout=timeout,
    )
    response.raise_for_status()

    size_mb = len(response.content) / (1024 * 1024)
    n_rows = response.content.count(b"\n")
    log.info("download_data.done", size_mb=f"{size_mb:.1f}", n_rows=n_rows)

    return response.content


def parse_codelist_xml(xml_content: bytes) -> pd.DataFrame:
    """Parse an SDMX codelist XML response into a DataFrame with columns [code, name]."""
    ns = {
        "message": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message",
        "structure": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure",
        "common": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common",
    }
    root = ElementTree.fromstring(xml_content)
    codes = root.findall(".//structure:Code", ns)
    rows = []
    for code in codes:
        code_id = code.attrib["id"]
        name_el = code.find("common:Name", ns)
        name = name_el.text if name_el is not None else ""
        rows.append({"code": code_id, "name": name})
    return pd.DataFrame(rows)


def download_codelist(codelist_path: str) -> pd.DataFrame:
    """Download and parse a codelist from the API."""
    url = f"{API_BASE_URL}/codelist/{codelist_path}"
    log.info("download_codelist.start", url=url)
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    df = parse_codelist_xml(response.content)
    log.info("download_codelist.done", codelist=codelist_path, n_codes=len(df))
    return df


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def run(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"un/{SNAPSHOT_VERSION}/energy_statistics_database.zip")

    # Download all annual energy statistics data.
    data = download_data()

    # Download codelists for human-readable dimension names.
    codelists = {name: download_codelist(path) for name, path in CODELISTS.items()}

    # Save data and codelists as a compressed ZIP file.
    with tempfile.TemporaryDirectory() as temp_dir:
        output_file = Path(temp_dir) / "energy_statistics_database.zip"
        with zipfile.ZipFile(output_file, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("energy_statistics_database.csv", data)
            for name, df in codelists.items():
                buf = StringIO()
                df.to_csv(buf, index=False)
                zf.writestr(f"codelist_{name}.csv", buf.getvalue())

        # Add file to DVC and upload to S3.
        snap.create_snapshot(filename=output_file, upload=upload)


if __name__ == "__main__":
    run()
