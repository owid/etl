"""Load the snapshot of Statistics Norway's historical statistics and create a meadow dataset.

Table 3.13 (an HTML page) gives yearly emigration from Norway from 1821. Before 1951, the
figures only count emigration to overseas countries (destinations outside Europe).
"""

import pandas as pd

from etl.helpers import PathFinder
from etl.snapshot import Snapshot

paths = PathFinder(__file__)


def parse_historical_statistics(snap: Snapshot):
    """Parse table 3.13: one row per year; the emigration column starts in 1821."""
    tables = pd.read_html(snap.path)
    # The page holds several small layout tables; the data table is the large one.
    df = max(tables, key=len)

    df.columns = range(df.shape[1])
    df = df.rename(columns={0: "year", 1: "population", 10: "emigration"})[["year", "population", "emigration"]]
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df = df.dropna(subset=["year"])
    df["year"] = df["year"].astype(int)
    for col in ["population", "emigration"]:
        df[col] = pd.to_numeric(df[col].astype(str).str.replace(" ", "").str.replace("\xa0", ""), errors="coerce")
    df = df.dropna(subset=["emigration"])

    tb = snap.read_from_df(df)

    assert tb["year"].min() == 1821, "Emigration series no longer starts in 1821."
    assert tb["population"].notna().all(), "Missing population values in emigration years."
    # Spot-checks: the first emigrant ship in 1825, and the all-time peak in 1882.
    assert tb.loc[tb["year"] == 1825, "emigration"].item() == 53
    assert tb.loc[tb["year"] == 1882, "emigration"].item() == 28_804
    return tb


def run() -> None:
    #
    # Load inputs.
    #
    snap = paths.load_snapshot("norway_emigration_historical_statistics.html")

    #
    # Process data.
    #
    tb = parse_historical_statistics(snap)

    #
    # Save outputs.
    #
    ds_meadow = paths.create_dataset(
        tables=[tb.format(["year"], short_name=paths.short_name)], default_metadata=snap.metadata
    )
    ds_meadow.save()
