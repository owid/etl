"""Load snapshots and create a meadow dataset."""

import re

import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder
from etl.snapshot import Snapshot

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def read_html_table(snap: Snapshot, header_rows: list[list[str]]) -> Table:
    """Read the only table in the snapshot's HTML page that starts with the given header rows.

    Columns are named after the header rows (joining the distinct labels of each column, without footnote markers).
    Empty spacer rows are removed.

    """

    def _read_function(path):
        tables = [
            df
            for df in pd.read_html(path, thousands=",")
            if (len(df) > len(header_rows)) and all(df.iloc[i].tolist() == row for i, row in enumerate(header_rows))
        ]
        assert len(tables) == 1, f"Expected one table with the given header in {path}, found {len(tables)}."
        df = tables[0]
        columns = []
        for i in range(df.shape[1]):
            labels = list(dict.fromkeys(row[i] for row in header_rows))
            # Remove footnote markers (e.g. "Tonnage (1000 t)a").
            columns.append(" - ".join(re.sub(r"\)[ab]$", ")", label) for label in labels))
        df = df.iloc[len(header_rows) :].dropna(how="all").reset_index(drop=True)
        df.columns = columns
        return df

    return snap.read_custom(read_function=_read_function)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshots.
    snap_global = paths.load_snapshot("number_of_wild_fish_killed_for_food_global.html")
    snap_by_country = paths.load_snapshot("number_of_wild_fish_killed_for_food_by_country.html")
    snap_fishmeal = paths.load_snapshot("number_of_wild_fish_caught_for_fishmeal_and_fish_oil.html")

    # Read the data tables from the HTML pages.
    tb_global = read_html_table(
        snap=snap_global,
        header_rows=[
            [
                "Year",
                "FAO production tonnage",
                "Numbers (lower)  in billions",
                "Numbers (upper)  in billions",
                "Numbers (midpoint)  in billions",
            ]
        ],
    )
    tb_by_country = read_html_table(
        snap=snap_by_country,
        header_rows=[
            [
                "Country",
                "FAO production tonnage",
                "Numbers (lower)  in millions",
                "Numbers (upper)  in millions",
                "Numbers (midpoint)  in millions",
            ]
        ],
    )
    tb_fishmeal = read_html_table(
        snap=snap_fishmeal,
        header_rows=[
            [
                "Year",
                "Finfish capture for reduction",
                "Finfish capture for reduction",
                "Finfish capture for reduction",
                "Other non-food fisheries production (1000 t)a",
            ],
            [
                "Year",
                "Tonnage (1000 t)a",
                "Estimated numbers (billions)b",
                "Estimated numbers (billions)b",
                "Other non-food fisheries production (1000 t)a",
            ],
            [
                "Year",
                "Tonnage (1000 t)a",
                "Lower",
                "Upper",
                "Other non-food fisheries production (1000 t)a",
            ],
        ],
    )

    #
    # Process data.
    #
    # Improve table formats.
    # NOTE: Values are kept as strings, because the source includes rows of averages and totals, and values like "< 1".
    tb_global = tb_global.format(keys=["year"], short_name="number_of_wild_fish_killed_for_food_global")
    tb_by_country = tb_by_country.format(keys=["country"], short_name="number_of_wild_fish_killed_for_food_by_country")
    tb_fishmeal = tb_fishmeal.format(keys=["year"], short_name="number_of_wild_fish_caught_for_fishmeal_and_fish_oil")

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb_global, tb_by_country, tb_fishmeal])
    ds_meadow.save()
