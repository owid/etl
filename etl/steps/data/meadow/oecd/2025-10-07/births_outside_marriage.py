"""Load a snapshot and create a meadow dataset."""

from datetime import datetime

import pandas as pd

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

YEAR_NOW = datetime.now().year
# Common placeholders used in OECD data to represent missing values
PLACEHOLDERS = ["..", "—", "-", "…", "nan", ""]


def get_year_columns(tb, start_year=2001):
    """Extract year columns from table, filtering for valid years."""
    year_cols = []
    for col in tb.columns:
        col_str = str(col)
        if col_str.isdigit() and len(col_str) == 4 and start_year <= int(col_str) <= YEAR_NOW:
            year_cols.append(col)
    return year_cols


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("births_outside_marriage.xlsx")

    # Load data from snapshot.
    tb = snap.read(sheet_name="Births_outside_marriage", header=3)

    #
    # Process data.
    #

    # Get year columns that exist - check both string and integer formats
    year_cols = get_year_columns(tb)

    cols_to_keep = ["Country"] + year_cols

    tb = tb[cols_to_keep]
    # Convert year to integer
    tb = tb.dropna(subset=year_cols, how="all")

    tb["Country"] = tb["Country"].ffill()
    # Melt the data so year becomes a column
    tb = tb.melt(
        id_vars=["Country"],
        value_vars=year_cols,
        var_name="year",
        value_name="births_outside_marriage",
    )
    # Clean the value column - replace placeholders with NaN and convert to numeric
    tb["births_outside_marriage"] = tb["births_outside_marriage"].replace(PLACEHOLDERS, None)
    tb["births_outside_marriage"] = pd.to_numeric(tb["births_outside_marriage"], errors="coerce")

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb["births_outside_marriage"].metadata.origins = [snap.metadata.origin]

    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
