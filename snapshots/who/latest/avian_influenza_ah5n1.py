"""Script to create a snapshot of WHO's human cases with avian influenza A(H5N1).

This data is collected by the WHO, and summarised in PDF reports. CDC republishes the same data in a
machine-readable format, in the chart on:

    https://www.cdc.gov/bird-flu/php/surveillance/chart-epi-curve-ah5n1.html

The chart's "Download data (CSV)" button is rendered in the browser from the chart configuration
JSON below, which embeds the full data table. We fetch that configuration and write out its data
table, i.e. the same CSV the button produces.

Before running, check two things on the page:

- `date_published` in the DVC file: the "Last Updated" date shown at the top of the page. Read it
  without a browser with:

      curl -s https://www.cdc.gov/bird-flu/php/surveillance/chart-epi-curve-ah5n1.html \
        | grep -o '<meta property="cdc:last_updated"[^>]*>'

- `DATA_URL` below: CDC versions the chart configuration file (the current one ends in `_2.json`),
  so if the download starts failing, find the new one with:

      curl -s https://www.cdc.gov/bird-flu/php/surveillance/chart-epi-curve-ah5n1.html \
        | grep -o '/bird-flu/modules/charts/[^"]*\\.json'

NOTE: CDC's CDN returns 403 to browser-like User-Agents, but serves the default `requests` one
fine, so don't set a User-Agent header here.

Then run `etls who/latest/avian_influenza_ah5n1` and `etlr who/latest/avian_influenza_ah5n1`.

"""

import tempfile
from pathlib import Path

import click
import pandas as pd
import requests
from structlog import get_logger

from etl.helpers import PathFinder

log = get_logger()
paths = PathFinder(__file__)

# Configuration of the CDC chart that embeds the data table.
DATA_URL = "https://www.cdc.gov/bird-flu/modules/charts/past-reported-global-cases_2.json"


def fetch_data() -> pd.DataFrame:
    """Fetch the chart configuration and extract its embedded data table."""
    response = requests.get(DATA_URL, timeout=60)
    response.raise_for_status()
    data = response.json().get("data")

    assert data, f"No data table found in the chart configuration at {DATA_URL}."

    return pd.DataFrame(data)


def sanity_check(df: pd.DataFrame) -> None:
    """Check that the extracted table has the structure we expect."""
    assert list(df.columns[:2]) == ["Range", "Month"], f"Unexpected leading columns: {list(df.columns[:2])}."

    countries = [column for column in df.columns if column not in ("Range", "Month")]
    assert len(countries) >= 20, f"Expected at least 20 country columns, found {len(countries)}."

    # The table stacks monthly rows (each labelled with a date range, e.g. "2020-2024") on top of
    # yearly totals (labelled "All").
    n_yearly = (df["Range"] == "All").sum()
    assert n_yearly >= 25, f"Expected at least 25 yearly ('All') rows, found {n_yearly}."
    n_monthly = (~df["Range"].isin(["All", ""])).sum()
    assert n_monthly >= 300, f"Expected at least 300 monthly rows, found {n_monthly}."


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = paths.init_snapshot()

    # Fetch the data table published in the CDC chart.
    df = fetch_data()

    # Check the table before storing it.
    sanity_check(df)

    # Store the table as CSV, add file to DVC and upload to S3.
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir) / "avian_influenza_ah5n1.csv"
        df.to_csv(temp_path, index=False)
        snap.create_snapshot(filename=str(temp_path), upload=upload)

    log.info("Snapshot complete", n_rows=len(df))
