"""Load snapshots and create a meadow dataset with one table per source.

Each source has a different shape (some long on year, some wide on year,
some with no year column at all), so the index columns differ per table.
"""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


# Index columns per snapshot. Each snapshot becomes its own table.
SNAPSHOT_INDICES: dict[str, list[str]] = {
    "child_labor_us_long.csv": ["sex", "age_group", "source"],
    "child_labor_belgium.csv": ["age"],
    "child_labor_portugal.csv": ["sector", "year"],
    "child_labor_denmark.csv": ["year"],
    "child_labor_italy.csv": ["group", "year"],
    "child_labor_sweden.csv": ["year"],
    "child_labor_us_carter_sutch.csv": ["year"],
    "child_labor_portugal_goulart_bedi.csv": ["year"],
    "child_labor_england_wales_scotland.csv": ["occupation", "year"],
    "child_labor_england_wales.csv": ["age_group"],
    "child_labor_japan.csv": ["gender", "year"],
}


def run() -> None:
    tables = []
    for snapshot_name, index_cols in SNAPSHOT_INDICES.items():
        snap = paths.load_snapshot(snapshot_name)
        tb = snap.read()
        if snapshot_name == "child_labor_england_wales_scotland.csv":
            # Kirby's Table 1 reports occupation shares for the 1851 census.
            tb["year"] = 1851
        # Drop the redundant `child_labor_` prefix; the dataset already conveys it.
        short_name = snapshot_name.removesuffix(".csv").removeprefix("child_labor_")
        tb = tb.format(index_cols, short_name=short_name)
        tables.append(tb)

    ds_meadow = paths.create_dataset(tables=tables)
    ds_meadow.save()
