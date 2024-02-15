"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns to select from the data, and how to rename them.
COLUMNS = {
    "location": "location",
    "year": "year",
    "cumulative_mass_balance__gt": "cumulative_ice_sheet_mass_balance",
}


def convert_decimal_year_to_date(tb: Table) -> Table:
    tb = tb.copy()
    # Years are given as decimals, but they do not correspond to arbitrary times in the year.
    # They correspond to the beginning of each of the 12 months.
    tb["month"] = round((tb["year"] - tb["year"].astype(int)) * 12).astype(int) + 1
    assert set(tb["month"]) == set(range(1, 13)), "Inaccurate calculation of months."
    tb["year"] = tb["year"].astype(int)
    tb["date"] = pd.to_datetime(tb[["year", "month"]].assign(day=1))
    tb = tb.drop(columns=["year", "month"], errors="raise")
    return tb


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its tables.
    ds_meadow = paths.load_dataset("ice_sheet_mass_balance")
    tb = ds_meadow["ice_sheet_mass_balance"].reset_index()

    #
    # Process data.
    #
    # Select columns and rename them.
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Create a date column (given that "year" is given with decimals).
    tb = convert_decimal_year_to_date(tb=tb)

    # Set an appropriate index to each table and sort conveniently.
    tb = tb.set_index(["location", "date"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()
