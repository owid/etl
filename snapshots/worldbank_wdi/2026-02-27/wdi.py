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
import re
import tempfile
import time
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

# DDH is intermittently flaky (502 Bad Gateway, occasionally empty responses).
# Apply bounded retries with exponential backoff to any call hitting it.
DDH_RETRY_STATUSES = (500, 502, 503, 504)
DDH_RETRY_MAX_ATTEMPTS = 6
DDH_RETRY_BACKOFF_BASE = 2.0


def _ddh_get_json(url: str, timeout: int = 120) -> dict:
    """GET a DDH URL and decode JSON, retrying on transient errors."""
    last_err: Exception | None = None
    for attempt in range(DDH_RETRY_MAX_ATTEMPTS):
        try:
            response = requests.get(url, timeout=timeout)
            if response.status_code in DDH_RETRY_STATUSES:
                last_err = requests.HTTPError(f"{response.status_code} on {url}")
            else:
                response.raise_for_status()
                return response.json()
        except (requests.RequestException, json.JSONDecodeError) as e:
            last_err = e
        sleep_s = DDH_RETRY_BACKOFF_BASE**attempt
        print(
            f"  DDH request failed ({last_err!r}); retrying in {sleep_s:.0f}s [attempt {attempt + 1}/{DDH_RETRY_MAX_ATTEMPTS}]"
        )
        time.sleep(sleep_s)
    raise RuntimeError(f"DDH request failed after {DDH_RETRY_MAX_ATTEMPTS} attempts: {url}") from last_err


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


def _score_indicator_entry(indicator: dict) -> tuple[int, int, int, int]:
    """Score one DDH indicator entry for dedupe. Higher is better.

    Priority (lexicographic on the tuple):
      1. Has a non-empty `Long definition` — keeps rich entries over taxonomy-only stubs
         (`Code` + `Description` + `First level` + `Second level`).
      2. Highest `Base Period` year — resolves duplicates like
         `NY.GDP.PCAP.PP.KD` where WB ships both the legacy 2017-PPP entry and
         the new 2021-PPP entry; the 2017 one was winning under last-wins.
      3. Number of non-empty fields filled in.
      4. Total content length, as a final tiebreak.
    """
    has_long_def = 0
    base_period = 0
    n_filled = 0
    total_len = 0
    for field in indicator.get("fields", []):
        desc = (field.get("description") or "").strip()
        if not desc:
            continue
        n_filled += 1
        total_len += len(desc)
        name = field.get("name")
        if name == "Long definition":
            has_long_def = 1
        elif name == "Base Period":
            m = re.search(r"\d{4}", desc)
            if m:
                base_period = int(m.group(0))
    return (has_long_def, base_period, n_filled, total_len)


def add_json_metadata_to_zip(zip_path: Path) -> None:
    """Fetch JSON metadata from API and add it to the existing zip file."""
    all_indicators = []
    skip = 0
    batch_size = 1000

    # Fetch all indicators using pagination
    while True:
        url = f"{API_BASE_URL}?dataset_unique_id={DATASET_ID}&top={batch_size}&skip={skip}"
        batch_data = _ddh_get_json(url)

        # Check if we got any data
        if not batch_data.get("data") or len(batch_data["data"]) == 0:
            break

        all_indicators.extend(batch_data["data"])

        # If we got less than the batch size, we're done
        if len(batch_data["data"]) < batch_size:
            break

        skip += batch_size

    # The DDH API returns multiple entries per series code: typically several
    # revisions of the indicator's metadata (e.g. PPP 2017 vs PPP 2021 base year)
    # plus an occasional "classification-only" stub. WB hasn't yet exposed a
    # last_modified flag (see thread with apirlea@worldbank.org, Sep 2025), so
    # we pick the best entry per code via `_score_indicator_entry`.
    entries_by_code: dict[str, list[dict]] = {}
    for indicator in all_indicators:
        series_code = None
        for field in indicator.get("fields", []):
            if field.get("name") in ("Series Code", "Code"):
                series_code = field.get("description")
                break
        if series_code:
            entries_by_code.setdefault(series_code, []).append(indicator)

    # Stable tiebreak via index: when scores match, prefer the later occurrence
    # (preserves the previous "last-wins" behaviour for tied entries).
    deduped_indicators = [
        max(enumerate(entries), key=lambda x: (_score_indicator_entry(x[1]), x[0]))[1]
        for entries in entries_by_code.values()
    ]
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
    if response.status_code == 404:
        return None
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
    meta_orig = _ddh_get_json(URL_METADATA)

    assert snap.metadata.origin

    # Update the publication date to be the date of their latest update.
    last_updated = meta_orig.get("last_updated_date")
    # Handle timezone info by removing it before parsing
    if last_updated and "+00:00" in last_updated:
        last_updated = last_updated.replace("+00:00", "")
    snap.metadata.origin.date_published = dt.datetime.strptime(last_updated, "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%d")  # ty: ignore

    # Update the download URL to the latest version.
    # NOTE: The WB API returns a versioned URL (e.g. WDI_CSV_2026_01_29.zip) which lags behind
    # the actual latest release — it still pointed to the January data after the February update.
    # We use the generic URL (WDI_CSV.zip) instead, which always serves the latest data.
    # The condition preserves the generic URL if it's already set in the DVC file, preventing
    # this function from overwriting it with the stale versioned URL from the API.
    # If the World Bank ever fixes the API to return the latest release URL,
    # this condition and the generic URL in the DVC file can be removed.
    api_download_url = [r for r in meta_orig["Resources"] if r["name"] == "CSV file"][0]["url"]
    if "WDI_CSV.zip" not in str(snap.metadata.origin.url_download):
        snap.metadata.origin.url_download = api_download_url

    # Update the description (in case it changed).
    snap.metadata.origin.description = BeautifulSoup(
        meta_orig.get("identification").get("description"), features="html.parser"
    ).get_text()
    # Update the access date.
    snap.metadata.origin.date_accessed = dt.datetime.now().strftime("%Y-%m-%d")
    # Update the full citation.
    snap.metadata.origin.citation_full = (
        f"World Development Indicators (WDI), The World Bank ({snap.metadata.origin.date_published.split('-')[0]})."  # ty: ignore
    )

    # Sanity checks.
    error = "Citation has changed. Review the new citation and adjust the code."
    assert meta_orig["identification"]["citation"] == "World Development Indicators, The World Bank", error
    error = "License has changed. Review the new license and adjust the code."
    assert meta_orig["constraints"]["license"]["license_id"] == "Creative Commons Attribution 4.0", error


if __name__ == "__main__":
    main()
