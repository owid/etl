"""Script to create a snapshot of dataset. This is a successor of https://github.com/owid/importers/tree/master/who_gho.

There are three different Data APIs for Global Health Observatory:
- [GHO Odata API](https://www.who.int/data/gho/info/gho-odata-api)
- [Athena API](https://www.who.int/data/gho/info/athena-api)
- [DataDot](https://data.who.int/indicators)

The Athena API provides more intuitive access to CSV format than the Odata API. DataDot
is still in beta and does not provide access to all indicators.

Athen API links:
- [Examples](https://apps.who.int/gho/data/node.resources.examples?lang=en)
- [Indicator list](https://apps.who.int/gho/athena/api/GHO?format=json)
- [CSV Data for an indicator](http://apps.who.int/gho/athena/api/GHO/WHOSIS_000001?format=csv&profile=text)
- [Metadata HTML](https://www.who.int/data/gho/indicator-metadata-registry/imr-details/1)

Unfortunately the Athena API does not provide a way to download metadata, so we have to scrape it from
HTML page with metadata.

Another issue is that about 800 indicators in the list don't have `url` field with indicator ID, even though
their metadata page at https://www.who.int/data/gho/indicator-metadata-registry/imr-details/1 exists.
Because of that, we can't download the metadata. The workaround would be to match their names on
https://www.who.int/data/gho/data/indicators/indicators-index and grab metadata from the links on that page.
An example of such indicator without `url` field is HEMOGLOBINLEVEL_REPRODUCTIVEAGE_MEAN.
"""

import concurrent.futures
import html
import io
import json
import os
import urllib.parse
import zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict

import click
import html2text
import pandas as pd
import requests
import structlog
import tenacity
from bs4 import BeautifulSoup
from joblib import Memory
from owid import repack

from etl.paths import CACHE_DIR
from etl.snapshot import Snapshot

log = structlog.get_logger()

memory = Memory(CACHE_DIR, verbose=0)

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Parallelize downloads
MAX_WORKERS = 5

# Only include specified indicators, useful for debugging
SUBSET = os.environ.get("SUBSET")

# Labels of indicators that cannot be empty even if GHO API returns empty response
NON_EMPTY_LABELS = [
    "LIFE_0000000030",
    "NTD_YAWSNUM",
]


@memory.cache()
def load_indicators() -> pd.DataFrame:
    log.info("load_indicators")
    url_indicators = "https://apps.who.int/gho/athena/api/GHO?format=json"
    js = requests.get(url_indicators).json()

    indicators = js["dimension"][0]["code"]
    for ind in indicators:
        # explode attributes
        for attr in ind.pop("attr"):
            ind[attr["category"]] = attr["value"]

    df = pd.DataFrame(indicators)

    # exclude unnecessary columns
    # NOTE: DEFINITION_XML link is broken
    df = df.drop(columns=["display_sequence", "DISPLAY_FR", "DISPLAY_ES", "RENDERER_ID"])

    # unescape special characters
    df.display = df.display.map(lambda x: html.unescape(x) if x else x)

    # be consistent with indicators in this list https://www.who.int/data/gho/data/indicators/indicators-index
    df.display = df.display.str.replace("≥", ">=").str.replace("≤", "<=")

    return df


def load_metadata_urls() -> Dict[str, str]:
    """Load metadata URLs for indicators. This is more reliable than `url` field in the indicators list which
    could be sometimes empty."""
    log.info("load_metadata_urls")
    url = "https://www.who.int/data/gho/data/indicators/indicators-index"
    resp = requests.get(url)
    soup = BeautifulSoup(resp.text, "html.parser")

    meta_links = {}

    for a_href in soup.find_all("a", href=lambda href: href and "gho/data" in href):  # type: ignore
        link = a_href.attrs["href"]
        # relative link
        if link.startswith("/data/gho"):
            link = "https://www.who.int" + link
        meta_links[a_href.text.strip()] = link

    return meta_links


def load_metadata_urls_from_registry() -> Dict[str, str]:
    """Load metadata URLs for indicators. This is more reliable than `url` field in the indicators list which
    could be sometimes empty."""
    log.info("load_metadata_urls_from_registry")
    url = "https://www.who.int/data/gho/indicator-metadata-registry"
    resp = requests.get(url)
    soup = BeautifulSoup(resp.text, "html.parser")

    meta_links = {}

    for a_href in soup.find_all("a", href=lambda href: href and "data/gho" in href):  # type: ignore
        link = a_href.attrs["href"]
        # relative link
        if link.startswith("/data/gho"):
            link = "https://www.who.int" + link
        meta_links[a_href.text.strip()] = link

    return meta_links


class EmptyResponseError(Exception):
    pass


@tenacity.retry(
    wait=tenacity.wait_fixed(2),
    stop=tenacity.stop_after_attempt(5),
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
def fetch_metadata(url: str) -> str:
    """Get indicator's metadata in Markdown. We could also return it as JSON to make it easier to parse later."""
    log.info("fetch_metadata", url=url)

    resp = fetch_url_with_retry(url)

    soup = BeautifulSoup(resp.text, "html.parser")

    meta = {}
    for metadata_box in soup.find_all("div", {"class": "metadata-box"}):
        div_title = metadata_box.find("div", {"class": "metadata-title"})
        title = div_title.text

        # remove element
        div_title.decompose()

        # convert rest to markdown
        content = html2text.html2text(str(metadata_box), bodywidth=0).strip()
        stripped_title = title.strip().replace(":", "")

        # save to JSON
        meta[stripped_title] = content

    return json.dumps(meta)


def get_metadata_for_row(row: Any, name_to_metadata_url: Dict[str, str]) -> str:
    try:
        metadata_url = name_to_metadata_url[row.display]
    except KeyError:
        # get ID from atttributes
        if hasattr(row, "IMR_ID") and not pd.isnull(row.IMR_ID):
            metadata_url = f"https://www.who.int/data/gho/indicator-metadata-registry/imr-details/{row.IMR_ID}"
        elif row.url in ("0",):
            log.warning("Invalid metadata URL for indicator", indicator=row.display, url=row.url)
            return "{}"
        elif row.url:
            # fallback to `url`
            metadata_url = row.url
        elif row.DEFINITION_XML and not pd.isnull(row.DEFINITION_XML):
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
            return "{}"

    if metadata_url.startswith("https://cms.who.int/data/gho"):
        # metadata behind login
        return "{}"
    elif not _metadata_url_is_valid(metadata_url):
        log.warning("Invalid metadata URL for indicator", indicator=row.display, url=row.url)
        return "{}"
    else:
        return fetch_metadata(metadata_url)


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
def _fetch_and_repack_data(label: str) -> pd.DataFrame:
    log.info("fetch_data", label=label)
    url = f"https://apps.who.int/gho/athena/api/GHO/{label}?format=csv&profile=text"
    resp = fetch_url_with_retry(url, retry_empty=True)
    resp.raise_for_status()

    data = pd.read_csv(io.StringIO(resp.text))

    # make it smaller
    data = repack.repack_frame(data)

    return data


def fetch_data(label: str) -> pd.DataFrame:
    """Fetch data from GHO API. It's possible that the API returns an empty response even though it has the data (
    and there's no way to know if the empty data is real or not). In that case we'll retry a few times or infinite
    times if the label is in NON_EMPTY_LABELS.
    Successful fetches are cached, so if you run the snapshot script multiple times, it's likely that it will fill
    some of the empty datasets.
    """
    try:
        return _fetch_and_repack_data(label)
    except tenacity.RetryError as e:
        # just try again... maybe it'll succeed one day
        if label in NON_EMPTY_LABELS:
            log.warning("Required label is empty, trying again", label=label)
            return fetch_data(label)

        if isinstance(e.last_attempt.exception(), EmptyResponseError):
            log.warning("Empty response after retrying", label=label)
            return pd.DataFrame()
        else:
            # Not working, raise an error and return empty
            log.warning("Error after retrying", label=label, e=e.last_attempt.exception())
            return pd.DataFrame()


def add_df_to_zip(zipf: zipfile.ZipFile, fname: str, df: pd.DataFrame) -> None:
    buffer = io.BytesIO()
    df.reset_index(drop=True).to_feather(buffer)
    buffer.seek(0)
    zipf.writestr(fname, buffer.getvalue())


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option(
    "--cache/--no-cache",
    default=False,
    type=bool,
    help="Use cache. Useful for debugging. Don't use for the final snapshot.",
)
def main(upload: bool, cache: bool) -> None:
    # Clear cache.
    if not cache:
        memory.clear()

    # Create a new snapshot.
    snap = Snapshot(f"who/{SNAPSHOT_VERSION}/gho.zip")

    # Get a list of indicators.
    df_indicators = load_indicators()

    # Get a dictionary of indicator name -> metadata URL
    # name_to_metadata_url = load_metadata_urls()
    name_to_metadata_url = load_metadata_urls_from_registry()

    if SUBSET:
        df_indicators = df_indicators[df_indicators.label.isin(SUBSET.split(","))]

    # Download metadata for each of them and add it a JSON.
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        df_indicators["metadata"] = list(
            executor.map(
                lambda row: get_metadata_for_row(row, name_to_metadata_url),
                list(df_indicators.itertuples()),
            )
        )

    # Create a ZIP file
    snap.path.parent.mkdir(exist_ok=True, parents=True)
    snap.path.unlink(missing_ok=True)
    mode = "w"
    with zipfile.ZipFile(snap.path, mode, zipfile.ZIP_DEFLATED) as zipf:
        # Add indicators metadata.
        add_df_to_zip(zipf, "indicators.feather", df_indicators)

        if MAX_WORKERS == 1:
            # Download individual CSV files.
            for label in df_indicators.label:
                data = fetch_data(label)
                # skip empty indicators
                if data.empty:
                    continue
                add_df_to_zip(zipf, f"{label}.feather", data)
        else:
            # Download individual CSV files.
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = []

                # Fetch all data async
                for label in df_indicators.label:
                    future = executor.submit(fetch_data, label)
                    futures.append(future)

                future_to_label = {executor.submit(fetch_data, label): label for label in df_indicators.label}

                # Add data to ZIP
                for future in concurrent.futures.as_completed(future_to_label):
                    label = future_to_label[future]
                    data = future.result()
                    # skip empty indicators
                    if not data.empty:
                        add_df_to_zip(zipf, f"{label}.feather", data)

    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(snap.path, upload=upload)


if __name__ == "__main__":
    main()
