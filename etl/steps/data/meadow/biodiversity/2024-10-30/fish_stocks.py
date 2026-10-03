"""Meadow step for the RAM Legacy fish stocks dataset.

The source CSV uses one row per (fish stock, year). We rename the entity column
to `country` so the rest of ETL can use it as the index dimension, fix a
column name that contained a stray space in the source CSV, and store the
data without further transformation.
"""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    snap = paths.load_snapshot()
    tb = snap.read_csv()

    # Source CSV has a stray space in this column name.
    tb = tb.rename(
        columns={
            "Entity": "country",
            "Year": "year",
            "biomass_relative_to preferred_management_rate": "biomass_relative_to_preferred_management_rate",
        }
    )

    # Make low-cardinality string column categorical for compact storage.
    tb["country"] = tb["country"].astype("category")

    tb = tb.format(["country", "year"], short_name=paths.short_name)

    ds = paths.create_dataset(tables=[tb])
    ds.save()
