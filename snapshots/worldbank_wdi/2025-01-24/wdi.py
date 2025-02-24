"""Imports World Bank World Development Indicators to Snapshot."""

import datetime as dt
import json
from pathlib import Path

import click
import requests
from bs4 import BeautifulSoup

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# URL to the World Bank metadata API.
URL_METADATA = "https://datacatalogapi.worldbank.org/ddhxext/DatasetDownload?dataset_unique_id=0037712"


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
def main(upload: bool) -> None:
    # Initialize a new snapshot.
    snap = Snapshot(f"worldbank_wdi/{SNAPSHOT_VERSION}/wdi.zip")

    # Load metadata from the world bank API, and upload snapshot metadata.
    update_snapshot_metadata(snap=snap)

    # Update local snapshot metadata dvc file.
    snap.metadata.save()

    # Download the ~270MB zip data file.
    snap.download_from_source()

    # Create the snapshot and upload the data.
    snap.dvc_add(upload=upload)


def update_snapshot_metadata(snap: Snapshot) -> None:
    # Load WDI metadata from the json file using the World Bank API.
    meta_orig = json.loads(requests.get(URL_METADATA).content)
    # Update the publication date to be the date of their latest update.
    snap.metadata.origin.date_published = dt.datetime.strptime(
        meta_orig.get("last_updated_date"), "%Y-%m-%dT%H:%M:%S"
    ).strftime("%Y-%m-%d")
    # Update the download URL to the latest version.
    snap.metadata.origin.url_download = [r for r in meta_orig["resources"] if r["name"] == "CSV"][0]["distribution"][
        "url"
    ]
    # Update the description (in case it changed).
    snap.metadata.origin.description = BeautifulSoup(
        meta_orig.get("identification").get("description"), features="html.parser"
    ).get_text()
    # Update the access date.
    snap.metadata.origin.date_accessed = dt.datetime.now().strftime("%Y-%m-%d")
    # Update the full citation.
    snap.metadata.origin.citation_full = (
        f"World Development Indicators (WDI), The World Bank ({snap.metadata.origin.date_published.split('-')[0]})."
    )

    # Sanity checks.
    error = "Citation has changed. Review the new citation and adjust the code."
    assert meta_orig["identification"]["citation"] == "World Development Indicators, The World Bank", error
    error = "License has changed. Review the new license and adjust the code."
    assert meta_orig["constraints"]["license"]["license_id"] == "Creative Commons Attribution 4.0", error


if __name__ == "__main__":
    main()
