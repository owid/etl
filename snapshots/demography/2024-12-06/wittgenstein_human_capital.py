"""Script to create a snapshot of dataset."""

import os
import shutil
from pathlib import Path
from typing import Optional

import click
import requests

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Latest version available
URL_SERVER = "https://wicshiny2023.iiasa.ac.at/wcde-data/wcde-v3-batch"
INDICATORS = [
    "asfr",
    "assr",
    "bmys",
    "bpop",
    "bprop",
    "cbr",
    "cdr",
    "e0",
    "easfr",
    "eassr",
    "emacb",
    "emi",
    "epop",
    "etfr",
    "ggapedu15",
    "ggapedu25",
    "ggapmys15",
    "ggapmys25",
    "growth",
    "imm",
    "macb",
    "mage",
    "mys",
    "net",
    "netedu",
    "nirate",
    "odr",
    "pop-age-edattain",
    "pop-age-sex-edattain",
    "pop-age-sex",
    "pop-age",
    "pop-sex-edattain",
    "pop-sex",
    "pop-total",
    "pop",
    "prop",
    "pryl15",
    "ryl15",
    "sexratio",
    "tdr",
    "tfr",
    "ydr",
]
SCENARIOS = [
    "1",
    "2",
    "22",
    "23",
    "3",
    "4",
    "5",
]

# Batch with historical data
URL_SERVER_HISTORICAL = "https://wicshiny2023.iiasa.ac.at/wcde-data/data-batch/"
SCENARIOS_HISTORICAL = [
    "ssp1",
    "ssp2",
    "ssp3",
    "ssp4",
    "ssp5",
]
INDICATORS_HISTORICAL = [
    "asfr",
    "assr",
    "bmys",
    "bpop",
    "bprop",
    "cbr",
    "cdr",
    "e0",
    "easfr",
    "eassr",
    # "emacb",
    # "emi",
    "epop",
    "etfr",
    "ggapedu15",
    "ggapedu25",
    "ggapmys15",
    "ggapmys25",
    "growth",
    # "imm",
    "macb",
    "mage",
    "mys",
    "net",
    # "netedu",
    "nirate",
    "odr",
    # "pop-age-edattain",
    # "pop-age-sex-edattain",
    # "pop-age-sex",
    # "pop-age",
    # "pop-sex-edattain",
    # "pop-sex",
    # "pop-total",
    "pop",
    "prop",
    "pryl15",
    "ryl15",
    "sexratio",
    "tdr",
    "tfr",
    "ydr",
]


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--path-to-file", "-f1", type=str, help="Path to local data file.")
@click.option("--path-to-file-historical", "-f2", type=str, help="Path to local data file.")
def main(path_to_file: Optional[str], path_to_file_historical: str, upload: bool) -> None:
    if path_to_file is not None:
        # Create a new snapshot.
        snap = Snapshot(f"demography/{SNAPSHOT_VERSION}/wittgenstein_human_capital.zip")

        # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
        snap.create_snapshot(filename=path_to_file, upload=upload)

    if path_to_file_historical is not None:
        # Create a new snapshot.
        snap = Snapshot(f"demography/{SNAPSHOT_VERSION}/wittgenstein_human_capital_historical.zip")

        # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
        snap.create_snapshot(filename=path_to_file_historical, upload=upload)


def get_data(folder: str):
    """Download all files from the WCDE server.

    Relevant files:
        - All scenarios in SCENARIOS
        - All indicators in INDICATORS

    Saves all these files in `folder`.
    """
    os.makedirs(folder, exist_ok=True)
    for scenario in SCENARIOS:
        for indicator in INDICATORS:
            url = f"{URL_SERVER}/{scenario}/{indicator}.rds"
            try:
                file_path = f"{folder}/{scenario}_{indicator}.rds"
                response = requests.get(url)
                response.raise_for_status()  # Raise HTTPError for bad responses
                with open(file_path, "wb") as file:
                    file.write(response.content)
                print(f"Saved: {file_path}")
            except requests.exceptions.RequestException as e:
                print(f"Failed to download {url}: {e}")

    # Zip file
    shutil.make_archive(folder, "zip", folder)


def get_data_historical(folder: str):
    """Download all files from the WCDE server (historical values).

    Relevant files:
        - All scenarios in SCENARIOS_HISTORICAL
        - All indicators in INDICATORS

    Saves all these files in `folder`.
    """
    os.makedirs(folder, exist_ok=True)
    for scenario in SCENARIOS_HISTORICAL:
        for indicator in INDICATORS_HISTORICAL:
            url = f"{URL_SERVER_HISTORICAL}/{scenario}/{indicator}.csv.gz"
            try:
                file_path = f"{folder}/{scenario}_{indicator}.csv.gz"
                response = requests.get(url)
                response.raise_for_status()  # Raise HTTPError for bad responses
                with open(file_path, "wb") as file:
                    file.write(response.content)
                print(f"Saved: {file_path}")
            except requests.exceptions.RequestException as e:
                print(f"Failed to download {url}: {e}")

    # Zip file
    shutil.make_archive(folder, "zip", folder)


if __name__ == "__main__":
    main()
