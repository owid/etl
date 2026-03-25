"""Script to create a snapshot of the World Bank ICP 2021 currency metadata.

Fetches the currency unit used per country in the ICP 2021 round from the World Bank API:
  https://api.worldbank.org/v2/sources/90/country/all/metatypes/CurrencyUnit?format=json

Each record maps a World Bank ISO3 country code to a currency string of the form "XXX: Currency Name".
"""

import json
import tempfile
from pathlib import Path

import click
import requests

from etl.snapshot import Snapshot

SNAPSHOT_VERSION = Path(__file__).parent.name
SOURCE_URL = "https://api.worldbank.org/v2/sources/90/country/all/metatypes/CurrencyUnit?format=json"


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def run(upload: bool) -> None:
    snap = Snapshot(f"worldbank_icp/{SNAPSHOT_VERSION}/icp_2021_currencies.json")

    response = requests.get(SOURCE_URL, timeout=30)
    response.raise_for_status()
    data = response.json()

    variables = data["source"][0]["concept"][0]["variable"]
    records = []
    for v in variables:
        value = v["metatype"][0]["value"]  # e.g. "USD: US Dollar"
        code, _, name = value.partition(": ")
        records.append(
            {
                "country_code": v["id"],  # WB ISO3 code
                "currency_code": code.strip(),
                "currency_name": name.strip(),
            }
        )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(records, f, indent=2, ensure_ascii=False)
        tmp_path = f.name

    snap.create_snapshot(filename=tmp_path, upload=upload)


if __name__ == "__main__":
    run()
