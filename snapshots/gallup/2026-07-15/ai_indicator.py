"""Script to create a snapshot of Gallup's AI-use-at-work indicator.

The data is the chart "1 in 10 U.S. Employees Use AI Daily in Their Role" on Gallup's
AI indicator page (url_main in the .dvc). The page server-renders each chart's data as
an HTML fallback table, and this script parses the one whose header matches the
chart's column labels — the same rows and labels as the page's manual "Get the data"
download, which earlier versions of this snapshot used.

NOTE: don't fetch the chart's Datawrapper CDN endpoint (datawrapper.dwcdn.net/a42MU/...)
instead — the published chart version there can lag the page's own data tables by a full
survey wave (observed 2026-07: the page tables already carried the May 2026 wave while
the latest published chart version still ended at February 2026).
"""

import io

import click
import pandas as pd
import requests

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Gallup's AI indicator page (same as url_main in the .dvc).
URL_MAIN = "https://www.gallup.com/699797/indicator-artificial-intelligence.aspx"

# Expected column labels of the AI-use trend chart, as shipped by Gallup.
EXPECTED_COLUMNS = ["Use of AI", "Daily AI users", "Frequent AI users", "Total AI users"]

TIMEOUT = 30


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def run(upload: bool = True) -> None:
    snap = paths.init_snapshot()

    df = fetch_chart_data()
    sanity_check(df)

    snap.create_snapshot(data=df, upload=upload)


def fetch_chart_data() -> pd.DataFrame:
    resp = requests.get(URL_MAIN, timeout=TIMEOUT)
    resp.raise_for_status()

    # The page holds one fallback table per embedded chart; select ours by its header.
    tables = pd.read_html(io.StringIO(resp.text))
    matches = [tb for tb in tables if list(tb.columns) == EXPECTED_COLUMNS]
    assert len(matches) == 1, f"Expected exactly one table with columns {EXPECTED_COLUMNS}, found {len(matches)}."

    return matches[0]


def sanity_check(df: pd.DataFrame) -> None:
    assert len(df) >= 7, f"Expected at least the 7 survey waves published up to May 2026, got {len(df)} rows."
    dates = pd.to_datetime(df["Use of AI"], format="%m/%d/%y")
    assert dates.is_monotonic_increasing and not dates.duplicated().any(), "Survey dates are not sorted and unique."
    shares = df[EXPECTED_COLUMNS[1:]]
    assert shares.notna().all().all(), "Missing share values."
    assert ((shares >= 0) & (shares <= 100)).all().all(), "Share values out of [0, 100]."
    # Daily users are a subset of frequent users, which are a subset of total users.
    assert (
        (df["Daily AI users"] <= df["Frequent AI users"]) & (df["Frequent AI users"] <= df["Total AI users"])
    ).all(), "Nested user shares are not ordered (daily <= frequent <= total)."
