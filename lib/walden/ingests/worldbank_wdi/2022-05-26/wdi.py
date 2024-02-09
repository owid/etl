"""Imports World Bank World Development Indicators to Walden.

Usage:

```
poetry run python -m ingests.worldbank_wdi
```
"""

import datetime as dt
import json
import tempfile
from pathlib import Path

import requests
import structlog
import yaml
from bs4 import BeautifulSoup
from owid.walden import add_to_catalog
from owid.walden.catalog import Dataset

log = structlog.get_logger()

URL_METADATA = "https://datacatalogapi.worldbank.org/ddhxext/DatasetDownload?dataset_unique_id=0037712"
MAX_RETRIES = 10
CHUNK_SIZE = 8192


def main():
    metadata = create_metadata()
    with tempfile.TemporaryDirectory() as temp_dir:
        # fetch the file locally
        assert metadata.source_data_url is not None and metadata.file_extension is not None
        content = download_file(metadata.source_data_url, MAX_RETRIES)
        output_file = Path(temp_dir) / f"data.{metadata.file_extension}"  # type: ignore
        with open(output_file, "wb") as f:
            f.write(content)

        # add it to walden, both locally and to our remote file cache
        add_to_catalog(metadata, output_file.as_posix(), upload=True)  # type: ignore


def create_metadata():
    meta = load_yaml_metadata()
    meta.update(load_external_metadata())

    return Dataset(
        **meta,
        date_accessed=dt.datetime.now().date().strftime("%Y-%m-%d"),
    )


def load_yaml_metadata() -> dict:
    fpath = Path(__file__).parent / f"{Path(__file__).stem}.meta.yml"
    with open(fpath) as istream:
        meta = yaml.safe_load(istream)

    return meta


def load_external_metadata() -> dict:
    meta_orig = json.loads(requests.get(URL_METADATA).content)
    # print({k: v for k, v in meta.items() if k not in ['indicators', 'resources', 'citation']})

    pub_date = dt.datetime.strptime(meta_orig.get("last_updated_date"), "%Y-%m-%dT%H:%M:%S").date()

    description = BeautifulSoup(meta_orig.get("identification").get("description"), features="html.parser").get_text()

    meta = {
        "name": f"World Development Indicators - World Bank ({pub_date.strftime('%Y.%m.%d')})",
        "description": description,
        "publication_year": pub_date.year,
        "publication_date": pub_date,
        "license_name": meta_orig.get("constraints").get("license").get("license_id"),
    }
    return meta


def download_file(url, max_retries: int, bytes_read: int = 0) -> bytes:
    """Downloads a file from a url.

    Retries download up to {max_retries} times following a ChunkedEncodingError
    exception.
    """
    log.info(
        "Downloading data...",
        url=url,
        bytes_read=bytes_read,
        remaining_retries=max_retries,
    )
    if bytes_read:
        headers = {"Range": f"bytes={bytes_read}-"}
    else:
        headers = {}

    content = b""
    try:
        with requests.get(url, headers=headers, stream=True) as r:
            r.raise_for_status()
            for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                bytes_read += CHUNK_SIZE
                content += chunk
    except requests.exceptions.ChunkedEncodingError:
        if max_retries > 0:
            log.info("Encountered ChunkedEncodingError, resuming download...")
            content += download_file(url, max_retries - 1, bytes_read)
        else:
            log.error(
                "Encountered ChunkedEncodingError, but max_retries has been "
                "exceeded. Download may not have been fully completed."
            )
    return content


if __name__ == "__main__":
    main()
