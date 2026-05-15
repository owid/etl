"""Load a snapshot and create a meadow dataset."""

import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    snap = paths.load_snapshot("us_patents.htm")

    # The snapshot is an HTML page; snap.read_csv etc. don't fit, so parse with pandas
    # and wrap the result. We attach origins explicitly afterwards so they propagate.
    html_tables = pd.read_html(snap.path)
    df = html_tables[2]

    tb = Table(df, short_name=paths.short_name, underscore=True)
    tb = tb.rename(columns={"calendar__year": "year"}).assign(country="United States").drop(columns="notes")

    # Clean `1836 (c) → 1836`.
    tb["year"] = tb["year"].str.replace("(c)", "", regex=False).astype(int)

    # Attach the snapshot's origin to every column.
    if snap.metadata.origin is not None:
        for col in tb.columns:
            tb[col].metadata.origins = [snap.metadata.origin]

    tb = tb.format(["country", "year"], short_name=paths.short_name)

    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)
    ds_meadow.save()
