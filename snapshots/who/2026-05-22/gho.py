"""Script to create a snapshot of dataset. This is a successor of https://github.com/owid/importers/tree/master/who_gho.

Data and metadata are pulled from two separate WHO endpoints:

- **Data**: the GHO OData API (https://www.who.int/data/gho/info/gho-odata-api). The
  indicator catalog at `/api/Indicator` exposes only `IndicatorCode` + `IndicatorName`
  per row — none of the legacy Athena fields (`url`, `IMR_ID`, `DEFINITION_XML`).
  Per-indicator data rows live at `/api/<IndicatorCode>`.
- **Metadata** (definitions, methodology, sources): WHO's Indicator Metadata Registry
  API at `https://www.who.int/api/multimedias/indicatormetadataregistrydefinitions`,
  which returns structured records keyed by IMR ID. We resolve `IndicatorCode → IMR ID`
  by name first (via the registry's own listing), then fall back to the `label →
  metadata URL` mapping from the previous snapshot for indicators whose names don't
  align. Unmatched indicators ship with empty metadata.

The legacy Athena API (`apps.who.int/gho/athena`) was taken offline mid-2026; that's
why this script no longer scrapes HTML metadata pages or relies on `DEFINITION_XML`
to bridge IndicatorCodes to IMR IDs. WHO's newer DataDot platform (https://data.who.int)
exposes rich metadata for ~60 flagship indicators only and is not used here.
"""

import concurrent.futures
import html
import io
import json
import os
import re
import time
import urllib.parse
import zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import click
import pandas as pd
import requests
import structlog
import tenacity
from joblib import Memory
from owid import repack

from etl.paths import CACHE_DIR
from etl.snapshot import Snapshot

log = structlog.get_logger()

memory = Memory(CACHE_DIR, verbose=0)

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Only include specified indicators, useful for debugging. Use SUBSET=debug to run the
# historical hand-picked debug subset.
SUBSET = os.environ.get("SUBSET")
if SUBSET == "debug":
    subset_list = [
        "WHS3_45",
        "PHE_HHAIR_POP_CLEAN_FUELS",
        "WHS3_56",
        "PHE_HHAIR_PROP_POP_CLEAN_FUELS",
        "NTD_YAWSNUM",
        "carep",
        "NTD_7",
        "NTD_8",
        "NTD_TRA5",
        "NTD_ONCHEMO",
        "NTD_ONCTREAT",
        "NCD_BMI_25A",
        "MDG_0000000026",
        "MDG_0000000032",
        "MORT_MATERNALNUM",
        "NUTSTUNTINGPREV",
        "R_Total_tax",
        "O_Group",
        "E_Group",
        "P_count_places_sf",
        "R_afford_gdp",
        "SDGNTDTREATMENT",
        "MH_1",
    ]
    SUBSET = ",".join(subset_list)

# Labels of indicators that cannot be empty even if GHO API returns empty response
NON_EMPTY_LABELS = [
    "LIFE_0000000030",
    "NTD_YAWSNUM",
]

# Current GHO data API. This replaced the retired Athena API for indicator data.
GHO_ODATA_BASE = "https://ghoapi.azureedge.net/api"

# Current WHO metadata registry API. This returns structured metadata records and is
# preferable to scraping the rendered HTML metadata pages.
METADATA_REGISTRY_API = "https://www.who.int/api/multimedias/indicatormetadataregistrydefinitions"

# Latest Athena-era snapshot used to restore the label -> metadata URL fields
# that Athena exposed. Data are fetched from the current GHO OData API.
METADATA_MAPPING_SNAPSHOT = "who/2025-05-19/gho.zip"


@memory.cache()
def load_metadata_mapping(_cache_version: int = 2) -> pd.DataFrame:
    """Load old Athena-era label -> metadata fields from the 2025-05-19 snapshot."""
    snap = Snapshot(METADATA_MAPPING_SNAPSHOT)
    snap.pull(force=False, retries=3)

    with zipfile.ZipFile(snap.path) as zf:
        with zf.open("indicators.feather") as f:
            mapping = pd.read_feather(f)

    columns = ["label", "display", "url", "DEFINITION_XML", "CATEGORY", "IMR_ID", "metadata"]
    mapping = mapping[columns].rename(columns={"metadata": "old_metadata"})
    return mapping.astype("string")


@memory.cache()
def load_indicators(_cache_version: int = 2) -> pd.DataFrame:
    """Load indicator list from the GHO OData API.

    The legacy Athena API (apps.who.int/gho/athena/api/GHO) was taken offline.
    We now use the OData API (ghoapi.azureedge.net/api/Indicator) which provides
    IndicatorCode (→ label) and IndicatorName (→ display). The other columns that
    the old Athena API returned (url, DEFINITION_XML, CATEGORY, IMR_ID) are kept
    so that downstream code (get_metadata_for_row, meadow step) still runs without
    changes. Metadata URL fields are filled from the extracted old-snapshot mapping.
    """
    log.info("load_indicators")
    url_indicators = f"{GHO_ODATA_BASE}/Indicator"
    js = requests.get(url_indicators, timeout=60).json()

    df = pd.DataFrame(js["value"])

    # Rename to match the schema expected by the rest of the script and meadow step.
    df = df.rename(columns={"IndicatorCode": "label", "IndicatorName": "display"})
    df = df.drop(columns=["Language"], errors="ignore")

    # Restore legacy metadata fields from the latest Athena-era snapshot mapping.
    metadata_mapping = load_metadata_mapping().drop(columns=["display"], errors="ignore")
    df = df.merge(metadata_mapping, on="label", how="left")

    # unescape special characters (IndicatorName may contain HTML entities)
    df["display"] = df["display"].map(lambda x: html.unescape(x) if x else x)

    # be consistent with indicators in this list https://www.who.int/data/gho/data/indicators/indicators-index
    df["display"] = df["display"].str.replace("≥", ">=").str.replace("≤", "<=")

    return df


def load_metadata_urls_from_registry() -> dict[str, str]:
    """Load metadata URLs for indicators from WHO's structured metadata registry API."""
    log.info("load_metadata_urls_from_registry")
    registry = load_metadata_registry()

    meta_links = {}
    for record in registry.itertuples():
        identifier = record.UrlName if pd.notna(record.UrlName) and record.UrlName else record.IMRID
        meta_links[record.Name] = f"https://www.who.int/data/gho/indicator-metadata-registry/imr-details/{identifier}"

    return meta_links


class EmptyResponseError(Exception):
    pass


@tenacity.retry(
    wait=tenacity.wait_exponential(multiplier=2, min=2, max=60),
    stop=tenacity.stop_after_attempt(8),
)
@memory.cache()
def fetch_url_with_retry(url: str, retry_empty: bool = False) -> requests.Response:
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    # sometimes GHO returns empty response even though it has the data and next requets will return it
    if retry_empty and resp.text == "":
        raise EmptyResponseError(f"Empty response for {url}")
    return resp


@memory.cache()
def load_metadata_registry() -> pd.DataFrame:
    """Load all current WHO metadata registry records from the structured API."""
    log.info("load_metadata_registry")
    rows = []
    top = 100
    skip = 0

    while True:
        resp = requests.get(METADATA_REGISTRY_API, params={"$top": top, "$skip": skip}, timeout=60)
        resp.raise_for_status()
        batch = resp.json().get("value", [])
        if not batch:
            break

        rows.extend(batch)
        if len(batch) < top:
            break
        skip += top

    return pd.DataFrame(rows).astype({"IMRID": "string", "UrlName": "string"})


def _metadata_url_identifier(url: str) -> str:
    """Extract the IMR id or slug from an imr-details URL."""
    parsed_url = urllib.parse.urlparse(url)
    path = urllib.parse.unquote(parsed_url.path.rstrip("/"))
    return path.rsplit("/", 1)[-1]


def _format_metadata_value(output_field: str, value: Any) -> str:
    """Format API values close to the old HTML-to-Markdown scraper output."""
    value = str(value).replace("<br>\r\n", "  \n").replace("<br>", "  \n").strip()
    value = re.sub(r"[ \t]{2,}(?!\n)", " ", value)

    if output_field in {"Other possible data sources", "Preferred data sources"}:
        value = " \n\n".join(part.strip() for part in value.split(",") if part.strip())
    elif output_field == "Contact person email" and "@" in value:
        value = f"[{value}](mailto:{value}?Subject=Global Health Observatory Enquiry)"

    return value


def _format_metadata_registry_record(record: pd.Series) -> str:
    """Convert a registry API record into the JSON shape used by previous snapshots."""
    field_mapping = {
        "Short name": "ShortName",
        "Data type": "indicatordatatypeString",
        "Topic": "TopicString",
        "Rationale": "Rationale",
        "Definition": "Definition",
        "Disaggregation": "Disaggregation",
        "Method of measurement": "MeasurementMethod",
        "Method of estimation": "MethodOfEstimation",
        "Method of estimation of global and regional aggregates": "MethodOfEstimationOfRegionalAndGlobalEstimates",
        "Other possible data sources": "OtherPossibleDataSourcesString",
        "Preferred data sources": "PreferredDataSourcesString",
        "Unit of Measure": "UnitOfMeasure",
        "Expected frequency of data dissemination": "ExpectedFrequencyOfDataDissemination",
        "Expected frequency of data collection": "ExpectedFrequencyOfDataCollection",
        "Comments": "Comments",
        "Contact person email": "ContactPersonEmail",
        "Name": "Name",
        "Data Type Representation": "indicatordatatyperepresentationString",
        "IMRID": "IMRID",
    }

    meta = {}
    for output_field, api_field in field_mapping.items():
        value = record.get(api_field)
        if pd.notna(value) and value not in ("", False):
            meta[output_field] = _format_metadata_value(output_field, value)

    return json.dumps(meta)


def _resolve_metadata_url_identifier(url: str) -> str:
    """Resolve WHO redirects and return the final IMR id or slug."""
    # Avoid rate-limiting from WHO's metadata pages when resolving old redirected IDs.
    time.sleep(1)
    resp = fetch_url_with_retry(url)
    return _metadata_url_identifier(resp.url)


@memory.cache()
def fetch_metadata(url: str) -> str:
    """Get indicator metadata from WHO's structured metadata registry API.

    The metadata record itself comes from the batched registry API. Per-indicator
    HTML pages are only requested when an old URL is missing from the registry API,
    so that WHO redirects like /65 -> /2977 can be resolved.
    """
    registry = load_metadata_registry()
    identifier = _metadata_url_identifier(url)

    # Most current URLs use either /imr-details/<IMRID> or /imr-details/<UrlName>.
    matches = registry[(registry["IMRID"] == identifier) | (registry["UrlName"] == identifier)]

    # Some old numeric URLs redirect to a new registry record, e.g. /65 -> /2977.
    # Resolve only when the original identifier was not present in the structured API.
    if matches.empty:
        log.info("resolve_metadata_redirect", url=url)
        resolved_identifier = _resolve_metadata_url_identifier(url)
        matches = registry[(registry["IMRID"] == resolved_identifier) | (registry["UrlName"] == resolved_identifier)]

    if matches.empty:
        return "{}"

    return _format_metadata_registry_record(matches.iloc[0])


def _previous_metadata_or_empty(row: Any) -> str:
    """Return previous snapshot metadata if available, otherwise an empty JSON object."""
    if hasattr(row, "old_metadata") and pd.notna(row.old_metadata) and row.old_metadata != "{}":
        log.warning("Falling back to previous snapshot metadata", indicator=row.display)
        return row.old_metadata
    return "{}"


def get_metadata_for_row(row: Any, name_to_metadata_url: dict[str, str]) -> str:
    # Prefer the Athena-era metadata URL extracted from the previous snapshot.
    # WHO redirects stale IMR URLs to the current metadata record where needed.
    if pd.notna(row.url) and row.url in ("0",):
        log.warning("Invalid metadata URL for indicator", indicator=row.display, url=row.url)
        return _previous_metadata_or_empty(row)
    elif pd.notna(row.url) and row.url:
        metadata_url = row.url
    elif hasattr(row, "IMR_ID") and pd.notna(row.IMR_ID):
        metadata_url = f"https://www.who.int/data/gho/indicator-metadata-registry/imr-details/{row.IMR_ID}"
    else:
        try:
            metadata_url = name_to_metadata_url[row.display]
        except KeyError:
            if pd.notna(row.DEFINITION_XML) and row.DEFINITION_XML:
                # extract ID from DEFINITION_XML like http://apps.who.int/gho/indicatorregistryservice/publicapiservice.asmx/IndicatorGetAsXml?profileCode=WHO&applicationCode=System&languageAlpha2=en&indicatorId=129
                try:
                    qs = urllib.parse.parse_qs(row.DEFINITION_XML)
                except Exception:
                    log.warning("Invalid DEFINITION_XML for indicator", indicator=row.display, url=row.DEFINITION_XML)
                    raise
                if "indicatorId" not in qs:
                    raise ValueError(f"No indicatorId in DEFINITION_XML: {row.DEFINITION_XML}")
                metadata_url = (
                    f"https://www.who.int/data/gho/indicator-metadata-registry/imr-details/{qs['indicatorId'][0]}"
                )
            else:
                log.warning("Metadata URL for indicator is empty", indicator=row.display)
                return _previous_metadata_or_empty(row)

    if metadata_url.startswith("https://cms.who.int/data/gho"):
        # metadata behind login
        return _previous_metadata_or_empty(row)
    elif not _metadata_url_is_valid(metadata_url):
        log.warning("Invalid metadata URL for indicator", indicator=row.display, url=row.url)
        return _previous_metadata_or_empty(row)
    else:
        metadata = fetch_metadata(metadata_url)
        if metadata != "{}":
            return metadata

        # If WHO's current metadata registry no longer exposes this old IMR id,
        # preserve the previous snapshot's metadata rather than dropping coverage.
        return _previous_metadata_or_empty(row)


def _metadata_url_is_valid(url: str) -> bool:
    ALLOWED_URLS = [
        "https://www.who.int/data/gho/indicator-metadata-registry/imr-details/",
        "https://www.who.int/data/gho/data/indicators/indicator-details/GHO",
        "https://www.who.int/data/gho/data/themes/",
    ]
    for allowed_url in ALLOWED_URLS:
        if allowed_url in url:
            return True
    return False


@memory.cache()
def fetch_dimensions():
    df = pd.DataFrame(requests.get(f"{GHO_ODATA_BASE}/Dimension").json()["value"])
    return df[["Code", "Title"]]


@memory.cache()
def fetch_dimension_values(dim_code: str):
    df = pd.DataFrame(requests.get(f"{GHO_ODATA_BASE}/DIMENSION/{dim_code}/DimensionValues").json()["value"])
    return df[["Code", "Title"]].set_index("Code")["Title"].to_dict()


@memory.cache()
def _fetch_and_repack_data(ind_code: str) -> pd.DataFrame:
    log.info("fetch_data", ind_code=ind_code)
    url = f"{GHO_ODATA_BASE}/{ind_code}?$format=json"
    resp = fetch_url_with_retry(url, retry_empty=True)
    resp.raise_for_status()

    data = pd.DataFrame(resp.json()["value"])

    # make it smaller
    if not data.empty:
        data = repack.repack_frame(data)

    return data


def fetch_data(ind_code: str) -> pd.DataFrame:
    """Fetch data from GHO API. It's possible that the API returns an empty response even though it has the data (
    and there's no way to know if the empty data is real or not). In that case we'll retry a few times or infinite
    times if the label is in NON_EMPTY_LABELS.
    Successful fetches are cached, so if you run the snapshot script multiple times, it's likely that it will fill
    some of the empty datasets.
    """
    try:
        return _fetch_and_repack_data(ind_code)
    except tenacity.RetryError as e:
        # just try again... maybe it'll succeed one day
        if ind_code in NON_EMPTY_LABELS:
            log.warning("Required indicator is empty, trying again", ind_code=ind_code)
            return fetch_data(ind_code)

        if isinstance(e.last_attempt.exception(), EmptyResponseError):
            log.warning("Empty response after retrying", ind_code=ind_code)
            return pd.DataFrame()
        else:
            # Not working, raise an error and return empty
            log.warning("Error after retrying", ind_code=ind_code, e=e.last_attempt.exception())
            return pd.DataFrame()


def fetch_and_process_data(ind_code: str) -> pd.DataFrame:
    # Fetch raw data
    df = fetch_data(ind_code)

    if df.empty:
        # Skip empty indicators
        log.warning("Empty indicator", ind_code=ind_code)
        return df

    # Remove unnecessary columns
    drop_cols = [
        "TimeDimensionBegin",
        "TimeDimensionEnd",
        "Date",
        "Id",
        "IndicatorCode",
        "DataSourceDimType",
        "DataSourceDim",
    ]
    df = df.drop(columns=drop_cols)

    dimensions = fetch_dimensions()

    # Turn dimensions into columns
    for k in (1, 2, 3):
        dim_type = f"Dim{k}Type"
        dim_col = f"Dim{k}"

        for dim_code in set(df[dim_type]):
            # Add dimension
            if not pd.isnull(dim_code):
                dim_title = dimensions[dimensions.Code == dim_code].Title.iloc[0]
                dim_values = fetch_dimension_values(dim_code)

                # Map codes to values
                df[dim_title] = df[dim_col].map(dim_values)

        # Drop dimension columns
        df = df.drop(columns=[dim_col, dim_type])

    # Turn spatial dimension into country
    mapping = {}
    for spatial_dim_code in set(df["SpatialDimType"]):
        mapping.update(fetch_dimension_values(spatial_dim_code))
    df["Country"] = df["SpatialDim"].map(mapping)

    # Turn time dimension into year
    mapping = {}
    for spatial_dim_code in set(df["TimeDimType"]):
        mapping.update(fetch_dimension_values(spatial_dim_code))
    df["Year"] = df["TimeDim"].astype("string").map(mapping)

    df = df.drop(columns=["TimeDim"])

    # make it smaller
    df = repack.repack_frame(df)

    return df


def add_df_to_zip(zipf: zipfile.ZipFile, fname: str, df: pd.DataFrame) -> None:
    buffer = io.BytesIO()
    df.reset_index(drop=True).to_feather(buffer)
    buffer.seek(0)
    zipf.writestr(fname, buffer.getvalue())


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option(
    "--clear-cache",
    is_flag=True,
    help="Clear the joblib cache before running. By default, cached metadata/data downloads are reused.",
)
@click.option(
    "--max-workers",
    default=1,
    show_default=True,
    type=int,
    help="Number of parallel workers for downloads.",
)
def main(upload: bool, clear_cache: bool, max_workers: int) -> None:
    # Reuse cache by default to avoid re-downloading thousands of WHO metadata pages.
    if clear_cache:
        memory.clear()

    # Create a new snapshot.
    snap = Snapshot(f"who/{SNAPSHOT_VERSION}/gho.zip")

    # Get a list of indicators.
    df_indicators = load_indicators()

    # Get a dictionary of indicator name -> metadata URL.
    name_to_metadata_url = load_metadata_urls_from_registry()

    if SUBSET:
        df_indicators = df_indicators[df_indicators.label.isin(SUBSET.split(","))]

    # Download metadata for each of them and add it a JSON. Keep this sequential
    # to avoid WHO rate-limiting the metadata registry pages.
    with ThreadPoolExecutor(max_workers=1) as executor:
        df_indicators["metadata"] = list(
            executor.map(
                lambda row: get_metadata_for_row(row, name_to_metadata_url),
                list(df_indicators.itertuples()),
            )
        )

    # Keep old metadata only as an internal fallback helper; don't persist it in
    # the new snapshot's indicator table.
    df_indicators = df_indicators.drop(columns=["old_metadata"], errors="ignore")

    # Create a ZIP file
    snap.path.parent.mkdir(exist_ok=True, parents=True)
    snap.path.unlink(missing_ok=True)
    mode = "w"
    with zipfile.ZipFile(snap.path, mode, zipfile.ZIP_DEFLATED) as zipf:
        # Add indicators metadata.
        add_df_to_zip(zipf, "indicators.feather", df_indicators)

        if max_workers == 1:
            # Download individual CSV files.
            for label in df_indicators.label:
                data = fetch_and_process_data(label)

                # skip empty indicators
                if data.empty:
                    continue
                add_df_to_zip(zipf, f"{label}.feather", data)
        else:
            # Download individual CSV files.
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_label = {
                    executor.submit(fetch_and_process_data, label): label for label in df_indicators.label
                }

                # Add data to ZIP
                for future in concurrent.futures.as_completed(future_to_label):
                    label = future_to_label[future]
                    data = future.result()
                    # skip empty indicators
                    if not data.empty:
                        add_df_to_zip(zipf, f"{label}.feather", data)

    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(snap.path, upload=upload)
