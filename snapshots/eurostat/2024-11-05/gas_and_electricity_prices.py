"""Script to create a snapshot of dataset."""

import zipfile
from pathlib import Path

import click
import requests
from tqdm.auto import tqdm

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Base URL for Eurostat API energy data.
BASE_URL = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/"

# List of dataset codes to download.
URL_DOWNLOADS = [
    ####################################################################################################################
    # Energy statistics - natural gas and electricity prices (from 2007 onwards) (nrg_pc)
    # Gas prices for household consumers - bi-annual data (from 2007 onwards) (nrg_pc_202)
    "nrg_pc_202",
    # Gas prices for non-household consumers - bi-annual data (from 2007 onwards) (nrg_pc_203)
    "nrg_pc_203",
    # Electricity prices for household consumers - bi-annual data (from 2007 onwards) (nrg_pc_204)
    "nrg_pc_204",
    # Electricity prices for non-household consumers - bi-annual data (from 2007 onwards) (nrg_pc_205)
    "nrg_pc_205",
    # Household consumption volumes of gas by consumption bands (nrg_pc_202_v)
    "nrg_pc_202_v",
    # Non-household consumption volumes of gas by consumption bands (nrg_pc_203_v)
    "nrg_pc_203_v",
    # Household consumption volumes of electricity by consumption bands (nrg_pc_204_v)
    "nrg_pc_204_v",
    # Non-household consumption volumes of electricity by consumption bands (nrg_pc_205_v)
    "nrg_pc_205_v",
    # Gas prices components for household consumers - annual data (nrg_pc_202_c)
    "nrg_pc_202_c",
    # Gas prices components for non-household consumers - annual data (nrg_pc_203_c)
    "nrg_pc_203_c",
    # Electricity prices components for household consumers - annual data (from 2007 onwards) (nrg_pc_204_c)
    "nrg_pc_204_c",
    # Electricity prices components for non-household consumers - annual data (from 2007 onwards) (nrg_pc_205_c)
    "nrg_pc_205_c",
    # Share for transmission and distribution in the network cost for gas and electricity - annual data (nrg_pc_206)
    "nrg_pc_206",
    ####################################################################################################################
    # Energy statistics - natural gas and electricity prices (until 2007) (nrg_pc_h)
    # Gas prices for domestic consumers - bi-annual data (until 2007) (nrg_pc_202_h)
    "nrg_pc_202_h",
    # Gas prices for industrial consumers - bi-annual data (until 2007) (nrg_pc_203_h)
    "nrg_pc_203_h",
    # Electricity prices for domestic consumers - bi-annual data (until 2007) (nrg_pc_204_h)
    "nrg_pc_204_h",
    # Electricity prices for industrial consumers - bi-annual data (until 2007) (nrg_pc_205_h)
    "nrg_pc_205_h",
    # Electricity - marker prices - bi-annual data (until 2007) (nrg_pc_206_h)
    "nrg_pc_206_h",
    ####################################################################################################################
]
# Further API parameters to download each file.
URL_SUFFIX = "?format=TSV&compressed=false"


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"eurostat/{SNAPSHOT_VERSION}/gas_and_electricity_prices.zip")

    # Ensure output snapshot folder exists, otherwise create it.
    snap.path.parent.mkdir(exist_ok=True, parents=True)

    # Create a temporary ZIP file.
    with zipfile.ZipFile(snap.path, "w") as zip_file:
        # Fetch all relevant datasets from Eurostat API.
        for code in tqdm(URL_DOWNLOADS):
            # Request the data file for the current dataset.
            response = requests.get(f"{BASE_URL}{code}{URL_SUFFIX}")
            # Save each file inside the ZIP file.
            file_name = f"{code}.tsv"
            zip_file.writestr(file_name, response.text)

    # Create snapshot and upload to R2.
    snap.create_snapshot(upload=upload, filename=snap.path)


if __name__ == "__main__":
    main()
