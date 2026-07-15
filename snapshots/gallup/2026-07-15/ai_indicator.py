"""Script to create a snapshot of Gallup's AI-use-at-work indicator.

The data is the chart "1 in 10 U.S. Employees Use AI Daily in Their Role" on Gallup's
AI indicator page (url_main in the .dvc). The chart is a Datawrapper embed (chart id
`a42MU`) whose data is served from Datawrapper's CDN: the pinned embed URL redirects
client-side to the latest published chart version, and that version's `dataset.csv`
(tab-separated) holds the same rows and column labels as the page's manual
"Get the data" download, which earlier versions of this snapshot used.

If the fetch breaks (e.g. Gallup rebuilds the chart under a new id), re-derive the
chart id from the page source — look for the Datawrapper embed of the AI-use trend
chart (data-src="https://datawrapper.dwcdn.net/<id>/...") — and update CHART_ID.
"""

import io
import re

import click
import pandas as pd
import requests

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Datawrapper chart id of "1 in 10 U.S. Employees Use AI Daily in Their Role" on the
# Gallup AI indicator page.
CHART_ID = "a42MU"

# Expected column labels, as shipped by Gallup's chart.
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
    # The pinned embed URL carries a client-side redirect to the latest published
    # version of the chart; resolve it to fetch the current data.
    embed = requests.get(f"https://datawrapper.dwcdn.net/{CHART_ID}/1/", timeout=TIMEOUT)
    embed.raise_for_status()
    match = re.search(rf"url=https://datawrapper\.dwcdn\.net/{CHART_ID}/(\d+)/", embed.text)
    assert match, f"Could not resolve the latest version of Datawrapper chart {CHART_ID}."
    version = match.group(1)

    data = requests.get(f"https://datawrapper.dwcdn.net/{CHART_ID}/{version}/dataset.csv", timeout=TIMEOUT)
    data.raise_for_status()

    return pd.read_csv(io.StringIO(data.text), sep="\t")


def sanity_check(df: pd.DataFrame) -> None:
    assert list(df.columns) == EXPECTED_COLUMNS, f"Unexpected columns: {list(df.columns)}."
    assert len(df) >= 6, f"Expected at least the 6 survey waves published up to February 2026, got {len(df)} rows."
    dates = pd.to_datetime(df["Use of AI"], format="%m/%d/%y")
    assert dates.is_monotonic_increasing and not dates.duplicated().any(), "Survey dates are not sorted and unique."
    shares = df[EXPECTED_COLUMNS[1:]]
    assert shares.notna().all().all(), "Missing share values."
    assert ((shares >= 0) & (shares <= 100)).all().all(), "Share values out of [0, 100]."
    # Daily users are a subset of frequent users, which are a subset of total users.
    assert (
        (df["Daily AI users"] <= df["Frequent AI users"]) & (df["Frequent AI users"] <= df["Total AI users"])
    ).all(), "Nested user shares are not ordered (daily <= frequent <= total)."
