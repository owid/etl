"""Load a meadow dataset and create a garden dataset."""

from typing import List

import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns to select from the data, and how to rename them.
COLUMNS = {
    "year": "year",
    "month": "month",
    "average": "concentration",
    # The following column is loaded only to perform a sanity check.
    "decimal": "decimal",
}


def add_rolling_average(tb: Table, original_column_names: List[str]) -> Table:
    tb_with_average = tb.copy()

    # Create a date range of each month (on the 15th).
    # NOTE: The minimum date in the data is "2001-01-15", however, when passing this date to pd.date_range with
    # freq="MS", the first point is dismissed because it is not the start of a month. For that reason, we shift the
    # first point to be at the beginning of the month.
    date_range = pd.date_range(
        start=tb_with_average["date"].min() - pd.tseries.offsets.MonthBegin(1),
        end=tb_with_average["date"].max(),
        freq="MS",
    ) + pd.DateOffset(days=14)

    # Get unique locations.
    unique_locations = tb_with_average["location"].unique()

    # Set date as index and sort.
    tb_with_average = tb_with_average.set_index(["location", "date"]).sort_index()

    # Create a MultiIndex with all possible combinations of date and location.
    multi_index = pd.MultiIndex.from_product([unique_locations, date_range], names=["location", "date"])

    # Reindex using the MultiIndex.
    tb_with_average = tb_with_average.reindex(multi_index)

    for original_column_name in original_column_names:
        # Create a rolling average with a window of one year, linearly interpolating missing values.
        # NOTE: Currently no interpolation is needed, as no data points are missing (and in fact date_range is identical
        # to the dates in the data). However, we need to interpolate in case there are missing points. Otherwise all
        # points after the missing one will be nan.
        tb_with_average[f"{original_column_name}_yearly_average"] = (
            tb_with_average[original_column_name]
            .interpolate("linear")
            .rolling(12)
            .mean()
            .copy_metadata(tb_with_average[original_column_name])
        )

    # Drop empty rows.
    tb_with_average = tb_with_average.dropna(subset=original_column_names, how="all").reset_index()

    # Sort conveniently.
    tb_with_average = tb_with_average.sort_values(["location", "date"]).reset_index(drop=True)

    for original_column_name in original_column_names:
        # Check that the values of the original column have not been altered.
        error = f"The values of the original {original_column_name} column have been altered."
        assert tb_with_average[original_column_name].astype(float).equals(tb[original_column_name].astype(float)), error

    return tb_with_average


def prepare_gas_data(tb: Table) -> Table:
    tb = tb.copy()

    # Extract gas name from table's short name.
    gas = tb.metadata.short_name.split("_")[0]

    # Columns to select from the data, and how to rename them.
    columns = {
        "year": "year",
        "month": "month",
        "average": f"{gas}_concentration",
        # The following column is loaded only to perform a sanity check.
        "decimal": "decimal",
    }

    # Select necessary columns and rename them.
    tb = tb[list(columns)].rename(columns=columns, errors="raise")

    # There is a "decimal" column for the year as a decimal number, that only has 12 possible values, corresponding to
    # the middle of each month, so we will assume the 15th of each month.
    error = "Date format has changed."
    assert len(set(tb["decimal"].astype(str).str.split(".").str[1])) == 12, error
    assert set(tb["month"]) == set(range(1, 13)), error
    tb["date"] = pd.to_datetime(tb[["year", "month"]].assign(day=15))

    # Remove unnecessary columns.
    tb = tb.drop(columns=["year", "month", "decimal"], errors="raise")

    # Add a location column.
    tb["location"] = "World"

    # Add a column with a rolling average for each gas.
    tb = add_rolling_average(tb=tb, original_column_names=[f"{gas}_concentration"])

    return tb


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("ghg_concentration")
    tb_co2 = ds_meadow["co2_concentration_monthly"].reset_index()
    tb_ch4 = ds_meadow["ch4_concentration_monthly"].reset_index()
    tb_n2o = ds_meadow["n2o_concentration_monthly"].reset_index()

    #
    # Process data.
    #
    # Prepare data for each gas.
    tb_co2 = prepare_gas_data(tb=tb_co2)
    tb_ch4 = prepare_gas_data(tb=tb_ch4)
    tb_n2o = prepare_gas_data(tb=tb_n2o)

    # Combine data for different gases.
    tb = tb_co2.merge(tb_ch4, how="outer", on=["location", "date"]).merge(
        tb_n2o, how="outer", on=["location", "date"], short_name=paths.short_name
    )

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["location", "date"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()
