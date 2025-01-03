"""Imports World Bank World Development Indicators to Snapshot."""

import datetime as dt
import json
from pathlib import Path

import click
import requests
import structlog
from bs4 import BeautifulSoup
from owid.catalog import License

from etl.snapshot import Snapshot

log = structlog.get_logger()

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

URL_METADATA = "https://datacatalogapi.worldbank.org/ddhxext/DatasetDownload?dataset_unique_id=0037712"


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"worldbank_wdi/{SNAPSHOT_VERSION}/wdi.zip")

    external_meta = load_external_metadata()
    for k, v in external_meta.items():
        setattr(snap.metadata, k, v)
    snap.metadata.save()

    snap.download_from_source()

    snap.dvc_add(upload=upload)


def load_external_metadata() -> dict:
    meta_orig = json.loads(requests.get(URL_METADATA).content)

    pub_date = dt.datetime.strptime(meta_orig.get("last_updated_date"), "%Y-%m-%dT%H:%M:%S").date()

    description = BeautifulSoup(meta_orig.get("identification").get("description"), features="html.parser").get_text()

    meta = {
        "name": f"World Development Indicators - World Bank ({pub_date.strftime('%Y.%m.%d')})",
        "description": description,
        "publication_year": pub_date.year,
        "publication_date": pub_date,
        "license": License(name=meta_orig.get("constraints").get("license").get("license_id")),
    }
    return meta


if __name__ == "__main__":
    main()
