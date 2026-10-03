"""Script to create a snapshot of dataset.

The data lives in an interactive table on the Pew Research Center fact sheet. The table is rendered
server-side into the page HTML as a set of `<td data-prc-v-col="N">` cells (col 0 = jurisdiction,
col 1 = year same-sex marriage took effect, col 2 = region, col 3 = notes). We fetch the page and
parse those cells directly, so future updates only require re-running this script (no manual
copy-paste from the browser).
"""

import re
from html import unescape
from pathlib import Path

import click
import pandas as pd
import requests

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Page hosting the interactive table.
SOURCE_URL = "https://www.pewresearch.org/religion/fact-sheet/same-sex-marriage-around-the-world/"

# Browser-like User-Agent: Pew's CDN returns 403 to the default requests UA.
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120 Safari/537.36"
    )
}

# Minimum number of jurisdictions expected (fails loudly if the page layout changes and parsing breaks).
MIN_EXPECTED_COUNTRIES = 35


def extract_table(html: str) -> pd.DataFrame:
    """Parse the country/year table from the fact-sheet HTML."""
    # Each table cell is tagged with its column index via `data-prc-v-col`.
    cells = re.findall(r'<td[^>]*data-prc-v-col="(\d+)"[^>]*>(.*?)</td>', html, re.DOTALL)

    # Reassemble cells into rows: a new row starts whenever column index resets to 0.
    rows: list[dict[int, str]] = []
    current: dict[int, str] = {}
    for col_str, content in cells:
        col = int(col_str)
        text = unescape(re.sub(r"<[^>]+>", "", content)).strip()
        if col == 0 and current:
            rows.append(current)
            current = {}
        current[col] = text
    if current:
        rows.append(current)

    # Keep only jurisdiction (col 0) and year (col 1).
    df = (
        pd.DataFrame(
            [{"country": r[0], "year": int(r[1])} for r in rows if 0 in r and 1 in r],
        )
        .sort_values("country")
        .reset_index(drop=True)
    )

    assert len(df) >= MIN_EXPECTED_COUNTRIES, (
        f"Parsed only {len(df)} jurisdictions (expected >= {MIN_EXPECTED_COUNTRIES}). The page layout may have changed."
    )
    assert df["year"].between(2000, int(SNAPSHOT_VERSION[:4])).all(), "Unexpected year values parsed from the table."

    return df


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def run(upload: bool) -> None:
    # Initialize a new snapshot.
    snap = Snapshot(f"pew/{SNAPSHOT_VERSION}/same_sex_marriage.csv")

    # Fetch the fact-sheet page and parse the table.
    response = requests.get(SOURCE_URL, headers=HEADERS, timeout=30)
    response.raise_for_status()
    df = extract_table(response.text)

    # Save snapshot.
    snap.create_snapshot(data=df, upload=upload)
