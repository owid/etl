"""Load snapshots and create a meadow dataset."""

import re

import pandas as pd
import pypdf
from owid.catalog import Table

from etl.helpers import PathFinder
from etl.snapshot import Snapshot

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns of the table of global numbers in the PDF supplement of Mood and Brooke (2024).
PDF_COLUMNS = [
    "year",
    "Capture production (landings) (million tonnes)",
    "Estimated numbers in millions (lower)",
    "Estimated numbers in millions (upper)",
    "Estimated numbers in millions (midpoint)",
    "% of estimate based on specific species/genus weight data (by tonnage)",
    "Mean individual fish weight for year (g) (lower)",
    "Mean individual fish weight for year (g) (upper)",
]


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


def read_pdf_table_of_yearly_global_numbers(snap: Snapshot) -> Table:
    """Read the rows of yearly data from the table of global numbers in the PDF supplement of Mood and Brooke (2024)."""
    with open(snap.path, "rb") as file:
        text = "".join(page.extract_text() for page in pypdf.PdfReader(file).pages)
    records = []
    for line in text.split("\n"):
        fields = line.split()
        # Rows of yearly data start with a year (rows of averages and differences are skipped).
        if fields and re.fullmatch(r"\d{4}", fields[0]):
            assert len(fields) == 8, f"Unexpected number of fields in row: {line}"
            values = [float(field.replace(",", "").replace("%", "")) for field in fields[1:]]
            records.append(dict(zip(PDF_COLUMNS, [int(fields[0])] + values)))
    tb = snap.read_from_records(records)
    assert list(tb["year"]) == list(range(1999, 2020)), "Unexpected years in the PDF table of global numbers."

    return tb


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshots.
    snap_global = paths.load_snapshot("number_of_wild_fish_killed_for_food_global.html")
    snap_by_country = paths.load_snapshot("number_of_wild_fish_killed_for_food_by_country.html")
    snap_fishmeal = paths.load_snapshot("number_of_wild_fish_caught_for_fishmeal_and_fish_oil.html")
    # Mood and Brooke (2024), used for the world totals before 2003.
    snap_paper = paths.load_snapshot("number_of_wild_fish_killed_for_food_global.pdf")

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
    tb_paper = read_pdf_table_of_yearly_global_numbers(snap=snap_paper)

    #
    # Process data.
    #
    # Improve table formats.
    # NOTE: Columns with rows of averages and totals, or values like "< 1", stay as strings; garden cleans them.
    tb_global = tb_global.format(keys=["year"], short_name="number_of_wild_fish_killed_for_food_global")
    tb_by_country = tb_by_country.format(keys=["country"], short_name="number_of_wild_fish_killed_for_food_by_country")
    tb_fishmeal = tb_fishmeal.format(keys=["year"], short_name="number_of_wild_fish_caught_for_fishmeal_and_fish_oil")
    tb_paper = tb_paper.format(keys=["year"], short_name="number_of_wild_fish_killed_for_food_global_from_paper")

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb_global, tb_by_country, tb_fishmeal, tb_paper])
    ds_meadow.save()
