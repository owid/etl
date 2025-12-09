"""Load a snapshot and create a meadow dataset."""

from datetime import datetime

import pandas as pd

from etl.helpers import PathFinder

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


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("children_in_families.xlsx")
    # Extract header row (row 3) and set it as column names
    header_row = 3
    # Load the main data sheet
    tb = snap.read(sheet_name="DistbnChildrenHouseholdType", header=header_row)

    #
    # Process data.
    #

    # Create the family status indicator from Unnamed: 2 and Unnamed: 3
    tb["presence_and_marital_status_of_parents"] = tb["Unnamed: 2"]

    # If Unnamed: 2 contains '-', use Unnamed: 3 instead
    mask = tb["Unnamed: 2"].astype(str).str.contains("-", na=False)
    tb.loc[mask, "presence_and_marital_status_of_parents"] = tb.loc[mask, "Unnamed: 3"]

    # Get year columns that exist - check both string and integer formats
    year_cols = get_year_columns(tb)

    # Select relevant columns
    cols_to_keep = ["Country", "presence_and_marital_status_of_parents"] + year_cols
    tb = tb[cols_to_keep]

    # Remove rows where all year columns are empty
    if year_cols:
        tb = tb.dropna(subset=year_cols, how="all")

    # Remove rows where family status is missing
    tb = tb.dropna(subset=["presence_and_marital_status_of_parents"])

    # Forward fill country names
    tb["Country"] = tb["Country"].ffill()

    # Melt the data so year becomes a column
    tb = tb.melt(
        id_vars=["Country", "presence_and_marital_status_of_parents"],
        value_vars=year_cols,
        var_name="year",
        value_name="value",
    )

    # Clean the value column - replace placeholders with NaN and convert to numeric
    tb["value"] = tb["value"].replace(PLACEHOLDERS, None)
    tb["value"] = pd.to_numeric(tb["value"], errors="coerce")

    # Remove rows with missing values
    tb = tb.dropna(subset=["value"])

    # Convert year to integer
    tb["year"] = tb["year"].astype(int)
    tb = tb.rename(columns={"presence_and_marital_status_of_parents": "indicator"})
    #
    # Save outputs.
    #
    tb["value"].metadata.origins = [snap.metadata.origin]

    tb = tb.format(["country", "year", "indicator"])

    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
