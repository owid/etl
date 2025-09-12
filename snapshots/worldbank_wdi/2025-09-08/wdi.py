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
import tempfile
import zipfile
from pathlib import Path

import click
import requests
from bs4 import BeautifulSoup

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# URL to the World Bank metadata API.
URL_METADATA = "https://ddh-openapi.worldbank.org/dataset/download?dataset_unique_id=0037712"

# Check out their API docs at https://ddh-openapi.worldbank.org/docs/index.html,
# use dataset id 0037712
API_BASE_URL = "https://ddh-openapi.worldbank.org/indicators"
DATASET_ID = "0037712"


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

    # Add JSON metadata from API to the zip file
    # NOTE: Ideally, we'd get all metadata from wdi.zip, but it contains outdated metadata.
    add_json_metadata_to_zip(snap.path)

    # Create the snapshot and upload the data.
    snap.dvc_add(upload=upload)


def add_json_metadata_to_zip(zip_path: Path) -> None:
    """Fetch JSON metadata from API and add it to the existing zip file."""
    all_indicators = []
    skip = 0
    batch_size = 1000

    # Fetch all indicators using pagination
    while True:
        url = f"{API_BASE_URL}?dataset_unique_id={DATASET_ID}&top={batch_size}&skip={skip}"
        response = requests.get(url)
        response.raise_for_status()
        batch_data = response.json()

        # Check if we got any data
        if not batch_data.get("data") or len(batch_data["data"]) == 0:
            break

        all_indicators.extend(batch_data["data"])

        # If we got less than the batch size, we're done
        if len(batch_data["data"]) < batch_size:
            break

        skip += batch_size

    # Deduplicate by series code - keep the last occurrence (likely most recent)
    unique_indicators = {}
    for indicator in all_indicators:
        # Find the series code in fields
        series_code = None
        for field in indicator.get("fields", []):
            if field.get("name") == "Series Code":
                series_code = field.get("description")
                break
            elif field.get("name") == "Code":
                series_code = field.get("description")
                break

        if series_code:
            unique_indicators[series_code] = indicator

    deduped_indicators = list(unique_indicators.values())
    print(
        f"After deduplication: {len(deduped_indicators)} unique indicators (removed {len(all_indicators) - len(deduped_indicators)} duplicates)"
    )

    # Create the complete metadata JSON with deduplicated indicators
    metadata_json = {"data": deduped_indicators, "count": len(deduped_indicators)}

    # Create a temporary file for the JSON metadata
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as temp_file:
        json.dump(metadata_json, temp_file, indent=2)
        temp_json_path = temp_file.name

    try:
        # Add the JSON file to the existing zip
        with zipfile.ZipFile(zip_path, "a") as zf:
            zf.write(temp_json_path, "WDIMetadata.json")
    finally:
        # Clean up temporary file
        Path(temp_json_path).unlink()


def update_snapshot_metadata(snap: Snapshot) -> None:
    # Load WDI metadata from the json file using the World Bank API.
    meta_orig = json.loads(requests.get(URL_METADATA).content)

    assert snap.metadata.origin

    # Update the publication date to be the date of their latest update.
    last_updated = meta_orig.get("last_updated_date")
    # Handle timezone info by removing it before parsing
    if last_updated and "+00:00" in last_updated:
        last_updated = last_updated.replace("+00:00", "")
    snap.metadata.origin.date_published = dt.datetime.strptime(last_updated, "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%d")  # type: ignore

    # Update the download URL to the latest version.
    snap.metadata.origin.url_download = [r for r in meta_orig["Resources"] if r["name"] == "CSV file"][0]["url"]

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
