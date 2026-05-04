"""Garden step for the RAM Legacy fish stocks dataset.

The data is one row per (fish stock, year). We do not run country harmonisation
because the entities here are individual fish stocks rather than countries.
We drop columns that have no data at all (some are referenced by the legacy
explorer but were never populated upstream) and pass everything else through.
"""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    ds_meadow = paths.load_dataset("fish_stocks")
    tb = ds_meadow.read("fish_stocks")

    # Drop columns that are fully empty in the source.
    empty_cols = [c for c in tb.columns if c not in ("country", "year") and tb[c].notna().sum() == 0]
    tb = tb.drop(columns=empty_cols)

    tb = tb.format(["country", "year"], short_name=paths.short_name)

    ds = paths.create_dataset(tables=[tb])
    ds.save()
