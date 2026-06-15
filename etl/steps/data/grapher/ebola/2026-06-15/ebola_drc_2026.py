"""Grapher step for the 2026 Bundibugyo Ebola outbreak.

Both tables are date-based. Grapher stores daily series as "days since a zero day", so we convert
the ``date`` column to an integer day offset and flag it with the ``zeroDay`` / ``yearIsDay`` display
options (the same convention used by other daily OWID datasets).
"""

import pandas as pd

from etl.helpers import PathFinder

paths = PathFinder(__file__)

ZERO_DAY = "2000-01-01"
TABLES = ["ebola_drc_2026", "ebola_drc_2026_by_health_zone"]


def run() -> None:
    #
    # Load inputs.
    #
    ds_garden = paths.load_dataset("ebola_drc_2026")

    #
    # Process data.
    #
    tables = []
    for table_name in TABLES:
        tb = ds_garden[table_name].reset_index()
        # The date index level comes back as a categorical; coerce to datetime before differencing.
        tb["year"] = (pd.to_datetime(tb["date"].astype(str)) - pd.Timestamp(ZERO_DAY)).dt.days
        tb = tb.drop(columns=["date"])
        tb = tb.format(["country", "year"], short_name=table_name)
        for column in tb.columns:
            tb[column].metadata.display = {"zeroDay": ZERO_DAY, "yearIsDay": True}
        tables.append(tb)

    #
    # Save outputs.
    #
    ds_grapher = paths.create_dataset(tables=tables, default_metadata=ds_garden.metadata)
    ds_grapher.save()
