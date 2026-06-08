"""World Bank PIP (Poverty and Inequality Platform) — external catalog mirror."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    # Load garden dataset.
    ds_garden = paths.load_dataset("world_bank_pip")

    # Read all tables from garden dataset.
    tables = [
        ds_garden.read(name, reset_index=False)
        for name in [
            "poverty",
            "incomes",
            "inequality",
            "cpi",
            "other_indicators",
            "survey_count",
            "percentiles",
            "complete_series",
        ]
    ]

    # Initialize a new external dataset.
    # Garden tables are already repacked, so skip it here to avoid the cost.
    ds = paths.create_dataset(tables=tables, repack=False)

    # Save external dataset.
    ds.save()
