"""Script to create a snapshot of the GWIS yearly burned area dataset.

Data is fetched from the GWIS country profile API, which returns annual totals by
land cover type and is more up to date than the bulk download ZIP on the downloads page.

API base: https://cprof.effis.emergency.copernicus.eu/api/v3/

The API only serves complete calendar years, so the range is derived from what it
returns rather than hardcoded: we ask for everything up to the current year and keep
the years it actually reports.
"""

import datetime as dt
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
# Upper bound of the request window. The API serves only complete years, so asking for the
# current year returns whatever it has, and the latest year is derived from the response.
YEAR_TO = dt.date.today().year

# A year is only kept if at least this share of countries report it. The API publishes a new
# year for all countries at once, so a year present for only a handful of them is a partial
# rollout, not a complete year.
MIN_COUNTRY_COVERAGE = 0.9


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

    tb = pd.DataFrame(rows)

    # Drop any trailing year the API has only partially published.
    coverage = tb.groupby("year")["iso3"].nunique() / tb["iso3"].nunique()
    complete = coverage[coverage >= MIN_COUNTRY_COVERAGE]
    assert not complete.empty, "No year is reported by enough countries; check the API response."
    latest = int(complete.index.max())
    dropped = sorted(y for y in tb["year"].unique() if y > latest)
    if dropped:
        log.info("Dropping partially published years", years=dropped)
        tb = tb[tb["year"] <= latest]

    # Guard against a silent truncation of the series.
    assert latest >= dt.date.today().year - 2, (
        f"Latest complete year is {latest}, more than two years behind. Has the API changed?"
    )
    expected = set(range(YEAR_FROM, latest + 1))
    missing = expected - set(tb["year"].unique())
    assert not missing, f"Missing years in the API response: {sorted(missing)}"

    log.info("Fetched burned area data", year_from=YEAR_FROM, year_to=latest, n_rows=len(tb))

    return tb


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
