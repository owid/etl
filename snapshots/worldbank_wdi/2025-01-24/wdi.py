"""Imports World Bank World Development Indicators to Snapshot.


It downloads both data and metadata in a single file, although metadata can also be retrieved via the API.
The WDI release notes (available at https://datatopics.worldbank.org/world-development-indicators/release-note-toc.html)
detail the latest additions, deletions, and modifications, which are crucial for updating the dataset.

Updating WDI:
1. Create a new version.
2. Update the 'url_download' in the metadata with the CSV link from:
   https://datacatalog.worldbank.org/search/dataset/0037712/World-Development-Indicators
3. Run the meadow and garden steps.
4. Execute `update_metadata.ipynb` in the garden step to refresh sources, years in metadata, and variable changes.
5. Report any quality issues to apirlea@worldbank.org and data@worldbank.org.


WDI also keeps large WDI archive at https://datatopics.worldbank.org/world-development-indicators/wdi-archives.html
"""

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
    # Note that metadata for indicators is loaded from the ZIP file, not in this function!
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

    assert snap.metadata.origin

    # Update the publication date to be the date of their latest update.
    snap.metadata.origin.date_published = dt.datetime.strptime(
        meta_orig.get("last_updated_date"), "%Y-%m-%dT%H:%M:%S"
    ).strftime("%Y-%m-%d")  # type: ignore
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
        f"World Development Indicators (WDI), The World Bank ({snap.metadata.origin.date_published.split('-')[0]})."  # type: ignore
    )

    # Sanity checks.
    error = "Citation has changed. Review the new citation and adjust the code."
    assert meta_orig["identification"]["citation"] == "World Development Indicators, The World Bank", error
    error = "License has changed. Review the new license and adjust the code."
    assert meta_orig["constraints"]["license"]["license_id"] == "Creative Commons Attribution 4.0", error


if __name__ == "__main__":
    main()
