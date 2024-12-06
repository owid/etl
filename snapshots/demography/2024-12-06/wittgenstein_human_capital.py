"""Script to create a snapshot of dataset."""

import os
import shutil
from pathlib import Path

import click
import requests

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

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


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--path-to-file", "-f", prompt=True, type=str, help="Path to local data file.")
def main(path_to_file: str, upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"demography/{SNAPSHOT_VERSION}/wittgenstein_human_capital.zip")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


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


if __name__ == "__main__":
    main()
