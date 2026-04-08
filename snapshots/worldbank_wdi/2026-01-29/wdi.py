"""Imports World Bank World Development Indicators to Snapshot.

## Known data quality issues

- `GC.TAX.TOTL.GD.ZS` (Tax revenue % of GDP): Values are corrupted - they are ~1e9 times smaller than they should be
  (e.g., US shows 1e-08 instead of ~11%). This affects the derived indicator `omm_tax_rev_percap`. The issue is in the
  raw World Bank data, not our processing. Previous version (2024-05-20) had correct values.
  Reported to World Bank: [TODO: add ticket number when reported]

It downloads both data and metadata in a single file, although metadata can also be retrieved via the API.
The WDI release notes (available at https://datatopics.worldbank.org/world-development-indicators/release-note-toc.html)
detail the latest additions, deletions, and modifications, which are crucial for updating the dataset.

You can also explore the WDI API directly at https://ddh-openapi.worldbank.org/docs/index.html. Use dataset id `0037712`.


## Updating WDI:

1. Create a new version and run the snapshot script. It updates the download link automatically.
2. Run the meadow and garden steps.
3. Update metadata using the CLI tool in the garden step:
   ```bash
   cd etl/steps/data/garden/worldbank_wdi/YYYY-MM-DD/
   python update_wdi_metadata.py update-titles --dry-run  # Preview changes
   python update_wdi_metadata.py update-titles            # Apply changes
   python update_wdi_metadata.py update-sources --dry-run # Preview GPT-4o-mini source updates
   python update_wdi_metadata.py update-sources           # Apply source updates (requires OPENAI_API_KEY)
   # Or run all at once:
   python update_wdi_metadata.py all --dry-run
   python update_wdi_metadata.py all --skip-charts        # Skip chart updates (defer to follow-up)
   ```
4. Report any quality issues to apirlea@worldbank.org and data@worldbank.org.


WDI also keeps large WDI archive at https://datatopics.worldbank.org/world-development-indicators/wdi-archives.html

## Upgrading indicators:

It's easier to do it in two steps:

1. Run indicator upgrader for:
    - GDP per capita, PPP (constant 2021 international $)
    - Current health expenditure per capita, PPP (current international $)

   There are tons of charts using these indicators, if a couple of them look good, it's safe to approve them all.

2. Run indicator upgrader for the rest
    - Auto-approve all charts with no changes
    - Manually review the rest


## Next update:

- Indicator `it_net_user_zs` (chart 755) still uses old version because the new one doesn't have regional aggregates.
  Is it still the case? If we calculate them ourselves, do they look ok?
- Write a script to auto-approve charts with no changes.
- ✅ Replaced update_metadata.ipynb with CLI tool (update_wdi_metadata.py) - see above for usage
- "dataPublisherSource" is no longer returned by WDI. Remove it if that's the case.
- Indicator metadata from downloaded ZIP file is outdated and we have to fetch metadata from API. Have they solved
    this problem? If yes, we can go back to ZIP file only.
- Check old WDI version and try to switch their charts to new indicators and archive them.

"""

import datetime as dt
import json
import tempfile
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import click
import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from etl.config import memory
from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# URL to the World Bank metadata API.
URL_METADATA = "https://ddh-openapi.worldbank.org/dataset/download?dataset_unique_id=0037712"

API_BASE_URL = "https://ddh-openapi.worldbank.org/indicators"
DATASET_ID = "0037712"

# Legacy API for individual indicator metadata (used by garden step)
LEGACY_API_BASE_URL = "https://api.worldbank.org/v2/indicator"

# Number of parallel workers for fetching legacy metadata
MAX_WORKERS = 20


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

    # Add legacy metadata from individual indicator API calls
    # This provides the exact format needed by the garden step (indicator_name, source, unit, topic)
    add_legacy_metadata_to_zip(snap.path)

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


@memory.cache
def fetch_single_indicator_metadata(indicator_code: str) -> dict | None:
    """Fetch metadata for a single indicator from the legacy World Bank API.

    Args:
        indicator_code: Indicator code in WDI format (e.g., "NY.GDP.MKTP.CD")

    Returns:
        Dictionary with indicator metadata or None if not available
    """
    api_url = f"{LEGACY_API_BASE_URL}/{indicator_code}?format=json"

    response = requests.get(api_url, timeout=30)
    response.raise_for_status()
    js = response.json()

    # API returns [pagination_info, [indicator_data]] or just [error_info]
    if len(js) < 2 or not js[1]:
        return None

    d = js[1][0]
    return {
        "indicator_code": indicator_code,
        "indicator_name": d.get("name"),
        "unit": d.get("unit"),
        "source": d.get("sourceOrganization"),
        "topic": d["topics"][0].get("value") if d.get("topics") else None,
    }


def fetch_legacy_metadata_parallel(indicator_codes: list[str], max_workers: int = MAX_WORKERS) -> list[dict]:
    """Fetch metadata for all indicators from legacy API using parallel requests.

    Args:
        indicator_codes: List of indicator codes in WDI format (e.g., ["NY.GDP.MKTP.CD", ...])
        max_workers: Maximum number of parallel workers

    Returns:
        List of metadata dictionaries (None values filtered out for indicators without metadata)
    """
    results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_code = {executor.submit(fetch_single_indicator_metadata, code): code for code in indicator_codes}

        for future in tqdm(as_completed(future_to_code), total=len(indicator_codes), desc="Fetching legacy metadata"):
            result = future.result()
            if result:
                results.append(result)

    return results


def extract_indicator_codes_from_zip(zip_path: Path) -> list[str]:
    """Extract unique indicator codes from WDICSV.csv in the ZIP file.

    Args:
        zip_path: Path to the WDI ZIP file

    Returns:
        List of indicator codes in WDI format (e.g., ["NY.GDP.MKTP.CD", ...])
    """
    with zipfile.ZipFile(zip_path, "r") as zf:
        df = pd.read_csv(zf.open("WDICSV.csv"), usecols=["Indicator Code"])
        return df["Indicator Code"].dropna().unique().tolist()


def add_legacy_metadata_to_zip(zip_path: Path) -> None:
    """Fetch legacy metadata for all indicators and add to ZIP as WDIMetadataLegacy.json."""
    # Extract indicator codes from the data file
    indicator_codes = extract_indicator_codes_from_zip(zip_path)
    print(f"Found {len(indicator_codes)} unique indicators in WDICSV.csv")

    # Fetch metadata in parallel
    metadata_list = fetch_legacy_metadata_parallel(indicator_codes)
    print(f"Successfully fetched metadata for {len(metadata_list)} indicators")

    # Create JSON structure
    legacy_metadata = {
        "data": metadata_list,
        "count": len(metadata_list),
        "fetched_at": dt.datetime.now().isoformat(),
    }

    # Write to temporary file and add to ZIP
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as temp_file:
        json.dump(legacy_metadata, temp_file, indent=2)
        temp_json_path = temp_file.name

    try:
        with zipfile.ZipFile(zip_path, "a") as zf:
            zf.write(temp_json_path, "WDIMetadataLegacy.json")
    finally:
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
