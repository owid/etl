"""Load a snapshot and create a meadow dataset."""

import numpy as np
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Expected index of row where years are located.
ROW_YEARS = 4


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load snapshot and read its data.
    snap = paths.load_snapshot("income_groups.xlsx")
    tb = snap.read(sheet_name="Country Analytical History")

    #
    # Process data.
    #
    # Sanity checks on input data.
    run_sanity_checks_on_inputs(tb=tb)

    # Extract years from one of the first rows.
    years = list(tb.loc[ROW_YEARS])

    # Rename columns.
    tb = tb.rename(columns={column: year for column, year in zip(tb.columns, years)}, errors="raise")
    tb = tb.rename(columns={tb.columns[0]: "country_code", tb.columns[1]: "country"}, errors="raise")

    # Keep only rows with data (which are rows that have a country code).
    tb = tb.dropna(subset=["country_code"]).reset_index(drop=True)

    # Pivot to have years as rows.
    # NOTE: Table in a Dataset can't have number columns (they will be converted, e.g. "2020" -> "_2020").
    tb = tb.melt(id_vars=["country_code", "country"], var_name="year", value_name="classification")

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = create_dataset(dest_dir, tables=[tb])
    ds_meadow.save()


def run_sanity_checks_on_inputs(tb: Table) -> None:
    # Sanity checks on input data.
    assert (num_rows := tb.shape[0]) == (
        num_rows_expected := 239
    ), f"Invalid number of rows. Expected was {num_rows_expected}, but found {num_rows}"
    assert (num_cols := tb.shape[1]) >= (
        num_cols_expected := 38
    ), f"Invalid number of columns. Expected was >{num_cols_expected}, but found {num_cols}"
    assert (
        tb.loc[10, "World Bank Analytical Classifications"] == "Afghanistan"
    ), "Row 10, column 'World Bank Analytical Classifications' expected to have value 'Afghanistan'"

    # Sanity check on years.
    years = list(tb.loc[ROW_YEARS])
    assert np.isnan(years[0]), f"The first column in row {ROW_YEARS} is expected to be NaN. Instead found {years[0]}"
    assert (
        years[1] == "Data for calendar year :"
    ), f"The second column in row {ROW_YEARS} is expected to be 'Data for calendar year :'. Instead found {years[1]}"
    assert all(
        isinstance(year, int) for year in years[2:]
    ), f"Columns 3 to the end in row {ROW_YEARS} should be numbers. Check: {years[2:]}"
