"""Load a snapshot and create a meadow dataset."""

import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns to use from the data, and how to rename them.
COLUMNS = {"Date": "date", "CW_2011": "sea_level_church_and_white_2011", "UHSLC_FD": "sea_level_uhslc"}


def fix_date_column(tb: Table) -> Table:
    tb = tb.copy()

    # Dates are given first in two different formats.
    # First they start in 1880-04-15 until 1899-10-15.
    # The next row is 1/15/00, referring to 1900-01-15.
    # This format goes on until 10/15/99, and then it jumps again to 1/15/00, which this time refers to 2000-01-15.

    # Detect the two instances where the date is 1/15/00.
    index_first, index_second = tb.index[tb["date"] == "1/15/00"]

    # Add a full year to those rows.
    tb.loc[(tb.index >= index_first) & (tb.index < index_second), "date"] = (
        tb["date"].str[0:-2] + "19" + tb["date"].str[-2:]
    )
    tb.loc[(tb.index >= index_second), "date"] = tb["date"].str[0:-2] + "20" + tb["date"].str[-2:]

    # Ensure all dates have a reasonable format.
    tb["date"] = pd.to_datetime(tb["date"]).dt.date.astype(str)

    return tb


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot and load data.
    snap = paths.load_snapshot("global_sea_level.csv")
    tb = snap.read(sep="\t", encoding_errors="ignore", usecols=["Date", "CW_2011", "UHSLC_FD"])

    #
    # Process data.
    #
    # Rename columns.
    tb = tb.rename(columns=COLUMNS, errors="raise")

    # Fix format of date column.
    tb = fix_date_column(tb=tb)

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["date"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_meadow.save()
