"""Script to create a snapshot of the "Main analysis" tables of a selection of Feedipedia feed composition pages.

Each page https://www.feedipedia.org/node/<id> holds one HTML table, whose first section is the "Main analysis".
The CSV keeps that section as published: one row per page and analysis, with Feedipedia's own labels and values.

"""

import io
import re
from html import unescape

import pandas as pd
import requests

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Feedipedia node ids of the tables to download.
NODE_IDS = [
    11682,
    12503,
    12663,
    16275,
    12420,
    12159,
    11640,
    20332,
    11997,
    12577,
    12262,
    12552,
    12359,
    11694,
    12463,
    12287,
    12246,
    11696,
]

# Header of the "Main analysis" section, as published (the last, unlabeled column holds an asterisk when the average was obtained by an equation).
COLUMNS = ["Main analysis", "Unit", "Avg", "SD", "Min", "Max", "Nb", "*"]


def fetch_main_analysis(node_id: int) -> pd.DataFrame:
    response = requests.get(f"https://www.feedipedia.org/node/{node_id}", timeout=60)
    response.raise_for_status()
    page = response.text

    # The table title is the page's main heading.
    heading = re.search(r"<h1 class='art-postheader'><span[^>]*>(.*?)</span></h1>", page, re.S)
    assert heading, f"No heading found on node {node_id}."

    # Read all cells as text, so that values are kept exactly as published.
    tables = pd.read_html(
        io.StringIO(page), match="Main analysis", thousands=None, converters={i: str for i in range(8)}
    )
    assert len(tables) == 1, f"Expected one composition table on node {node_id}, found {len(tables)}."
    tb = tables[0]
    assert tb.iloc[0].fillna("").tolist() == COLUMNS[:-1] + [""], f"Unexpected header on node {node_id}."

    # The "Main analysis" section ends at the first empty row (other sections, like minerals, follow).
    empty_rows = tb.index[tb.isna().all(axis=1)]
    end = empty_rows[0] if len(empty_rows) > 0 else len(tb)
    tb = tb.iloc[1:end].copy()
    tb.columns = COLUMNS
    tb.insert(0, "table_title", unescape(heading.group(1)).strip())
    tb.insert(0, "node_id", node_id)

    return tb


def run(upload: bool = True) -> None:
    """Create a new snapshot.

    Args:
        upload: Whether to upload the snapshot to S3.
    """
    # Init Snapshot object
    snap = paths.init_snapshot()

    # Download the "Main analysis" section of each table.
    df = pd.concat([fetch_main_analysis(node_id) for node_id in NODE_IDS], ignore_index=True)

    # Sanity checks: every page is present, with the main analyses and numeric averages.
    assert sorted(df["node_id"].unique()) == sorted(NODE_IDS), "Some pages are missing."
    for node_id, group in df.groupby("node_id"):
        avg = group.set_index("Main analysis")["Avg"]
        assert not avg.index.duplicated().any(), f"Duplicated analyses on node {node_id}."
        required = ["Dry matter", "Crude protein", "Ether extract", "Ash"]
        fiber = [label for label in ["NDF", "Neutral detergent fibre", "Crude fibre"] if label in avg.index]
        missing = [label for label in required if label not in avg.index]
        assert not missing and fiber, f"Node {node_id} lacks {missing or 'NDF and crude fibre'}."
        for label in required + fiber:
            assert pd.notna(pd.to_numeric(avg[label], errors="coerce")), f"Non-numeric {label} on node {node_id}."

    # Save snapshot.
    snap.create_snapshot(data=df, upload=upload)
