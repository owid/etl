"""Load a snapshot and create a meadow dataset."""

import re

import pandas as pd
from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Name of the data sheet. Besides it, the file has a cover sheet and a notes sheet.
SHEET_NAME = "RAS0101"

# Markers the producer uses in place of a value (e.g. "[x]" for not available, "[z]" for not applicable).
MISSING_VALUE_MARKERS = r"^\[[a-z]+\]$"


def find_header_row(snap_path, sheet_name: str) -> int:
    """Return the position of the row holding the column headers (the one starting with "Year").

    Accessible government spreadsheets put a few lines of text (title, notes, source) above the table, and their
    number can change between releases, so the header row is looked up rather than hard-coded.
    """
    # Read the raw sheet only to locate the header (the data itself is read with snap.read_excel, to keep origins).
    df_raw = pd.read_excel(snap_path, sheet_name=sheet_name, header=None, engine="odf")
    first_column = df_raw.iloc[:, 0].astype(str).str.strip().str.lower()
    header_rows = first_column[first_column.str.match(r"^year\b")].index.tolist()
    assert len(header_rows) == 1, (
        f"Expected one header row starting with 'Year' in sheet {sheet_name}, found {header_rows}."
    )
    return int(header_rows[0])


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("road_casualties.ods")

    # Check that the data sheet exists.
    sheet_names = pd.ExcelFile(snap.path, engine="odf").sheet_names
    assert SHEET_NAME in sheet_names, f"Sheet {SHEET_NAME} not found. Sheets in the file: {sheet_names}"

    # Load data from snapshot, skipping the lines of text above the table.
    header_row = find_header_row(snap.path, SHEET_NAME)
    tb = snap.read_excel(sheet_name=SHEET_NAME, skiprows=header_row, engine="odf")

    #
    # Process data.
    #
    # Remove note references from column names, e.g. "Pedestrians killed [note 3]" -> "Pedestrians killed".
    tb.columns = [re.sub(r"\s*\[note\s*\d+\]", "", str(column)).strip() for column in tb.columns]

    # Keep only rows with a year (drop any blank rows), and remove note references from years, e.g. "1939 [note 2]".
    tb = tb.rename(columns={tb.columns[0]: "year"}, errors="raise")
    tb["year"] = tb["year"].astype("string").str.extract(r"^\s*(\d{4})", expand=False)
    tb = tb.dropna(subset=["year"]).astype({"year": int})

    # Replace markers for unavailable data with NaN.
    tb = tb.replace(MISSING_VALUE_MARKERS, pd.NA, regex=True)

    # Make all data columns numeric (this fails loudly if any other text is left in a cell).
    for column in tb.columns.drop("year"):
        tb[column] = pr.to_numeric(tb[column])

    # Improve tables format.
    tables = [tb.format(["year"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
