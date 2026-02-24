"""Script to create a snapshot of the ISO 4217 currency list published by SIX Group.

The source XML is fetched from:
  https://www.six-group.com/dam/download/financial-information/data-center/iso-currrency/lists/list-one.xml

The XML is converted to a JSON array of records with fields:
  - country_name: country/territory name (upper-cased in source, preserved as-is)
  - currency_name: name of the currency (e.g. "Afghan Afghani")
  - currency_code: ISO 4217 3-letter alphabetic code (e.g. "AFN"), or null
  - currency_number: ISO 4217 numeric code (e.g. 971), or null
  - minor_units: number of decimal places (e.g. 2), or null for special cases
  - is_fund: true if the <CcyNm> element has IsFund="true" (special financial instruments, not everyday currencies)

The published date is taken from the XML root attribute and stored in the snapshot metadata.
"""

import json
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path

import click
import requests

from etl.snapshot import Snapshot

SNAPSHOT_VERSION = Path(__file__).parent.name
SOURCE_URL = "https://www.six-group.com/dam/download/financial-information/data-center/iso-currrency/lists/list-one.xml"


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def run(upload: bool) -> None:
    snap = Snapshot(f"iso/{SNAPSHOT_VERSION}/iso4217_currencies.json")

    # Fetch the XML from SIX Group.
    response = requests.get(SOURCE_URL, timeout=30)
    response.raise_for_status()

    root = ET.fromstring(response.content)

    records = []
    for entry in root.iter("CcyNtry"):

        def text(tag: str) -> str | None:
            el = entry.find(tag)
            return el.text.strip() if el is not None and el.text else None

        minor_raw = text("CcyMnrUnts")
        try:
            minor_units = int(minor_raw) if minor_raw is not None else None
        except ValueError:
            minor_units = None  # e.g. "N.A."

        ccy_nm_el = entry.find("CcyNm")
        is_fund = ccy_nm_el is not None and ccy_nm_el.get("IsFund") == "true"

        records.append(
            {
                "country_name": text("CtryNm"),
                "currency_name": text("CcyNm"),
                "currency_code": text("Ccy"),
                "currency_number": int(text("CcyNbr")) if text("CcyNbr") is not None else None,
                "minor_units": minor_units,
                "is_fund": is_fund,
            }
        )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(records, f, indent=2, ensure_ascii=False)
        tmp_path = f.name

    snap.create_snapshot(filename=tmp_path, upload=upload)


if __name__ == "__main__":
    run()
