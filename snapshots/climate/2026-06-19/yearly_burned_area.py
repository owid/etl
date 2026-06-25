"""Script to create a snapshot of the GWIS yearly burned area dataset.

Data is fetched from the GWIS country profile API, which returns annual totals by
land cover type and contains more recent data (including 2024) than the bulk download
ZIP available on the downloads page.

API base: https://cprof.effis.emergency.copernicus.eu/api/v3/

NOTE: Update YEAR_TO when a new year of data becomes available.
"""

import tempfile
import time
from pathlib import Path

import click
import pandas as pd
import requests
from structlog import get_logger

from etl.helpers import PathFinder

log = get_logger()
paths = PathFinder(__file__)

BASE_URL = "https://cprof.effis.emergency.copernicus.eu/api/v3"
LC_COLS = ["lc1", "lc2", "lc3", "lc4", "lc5"]
LC_NAMES = ["forest", "savannas", "shrublands_grasslands", "croplands", "other"]
YEAR_FROM = 2002
YEAR_TO = 2024


def get_countries() -> list:
    r = requests.get(f"{BASE_URL}/countries?env=PROD", timeout=30)
    r.raise_for_status()
    return r.json()


def get_banf(iso3: str) -> dict | None:
    url = f"{BASE_URL}/banf?level=ADM0&value={iso3}&year={YEAR_TO}&yearFrom={YEAR_FROM}&yearTo={YEAR_TO}&env=PROD"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json()


def fetch_all() -> pd.DataFrame:
    countries = get_countries()
    log.info("Fetching burned area data", n_countries=len(countries))

    rows = []
    for i, c in enumerate(countries):
        iso3 = c["iso3"]
        name = c["name"]
        log.info(f"Fetching {name}", iso3=iso3, progress=f"{i + 1}/{len(countries)}")
        data = get_banf(iso3)
        if data and data.get("banfyear"):
            for entry in data.get("banfyear", []):
                row = {"iso3": iso3, "country": name, "year": entry.get("year")}
                for lc, col_name in zip(LC_COLS, LC_NAMES):
                    row[col_name] = entry.get(lc, 0) or 0
                rows.append(row)
        time.sleep(0.2)

    return pd.DataFrame(rows)


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot.")
def main(upload: bool) -> None:
    snap = paths.init_snapshot()

    df = fetch_all()

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir) / "yearly_burned_area.csv"
        df.to_csv(tmp_path, index=False)
        snap.create_snapshot(filename=str(tmp_path), upload=upload)

    log.info("Snapshot complete", n_rows=len(df))
