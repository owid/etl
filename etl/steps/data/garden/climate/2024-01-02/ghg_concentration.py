"""Load a meadow dataset and create a garden dataset."""

from typing import List

import owid.catalog.processing as pr
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

    # Create a date range.
    date_range = pd.date_range(start=tb_with_average["date"].min(), end=tb_with_average["date"].max(), freq="1D")

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
        tb_with_average[f"{original_column_name}_yearly_average"] = (
            tb_with_average[original_column_name]
            .interpolate(method="linear")
            .rolling(365)
            .mean()
            .copy_metadata(tb_with_average[original_column_name])
        )

    # Drop empty rows.
    tb_with_average = tb_with_average.dropna(subset=original_column_names, how="all").reset_index()

    # Remove rolling average for the first year, given that it is based on incomplete data.
    for original_column_name in original_column_names:
        tb_with_average.loc[
            tb_with_average["date"] < tb_with_average["date"].min() + pd.Timedelta(days=365),
            f"{original_column_name}_yearly_average",
        ] = None

    # Sort conveniently.
    tb_with_average = tb_with_average.sort_values(["location", "date"]).reset_index(drop=True)

    for original_column_name in original_column_names:
        # Check that the values of the original column have not been altered.
        error = f"The values of the original {original_column_name} column have been altered."
        assert tb_with_average[original_column_name].astype(float).equals(tb[original_column_name].astype(float)), error

    return tb_with_average


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
    # Add a column for the gas name, and concatenate all tables.
    tb = pr.concat(
        [tb_co2.assign(gas="co2"), tb_ch4.assign(gas="ch4"), tb_n2o.assign(gas="n2O")],
        ignore_index=True,
        short_name=paths.short_name,
    )

    # Select necessary columns and rename them.
    tb = tb[list(COLUMNS) + ["gas"]].rename(columns=COLUMNS, errors="raise")

    # There is a "decimal" column for the year as a decimal number, that only has 12 possible values, corresponding to
    # the middle of each month, so we will assume the 15th of each month.
    error = "Date format has changed."
    assert len(set(tb["decimal"].astype(str).str.split(".").str[1])) == 12, error
    assert set(tb["month"]) == set(range(1, 13)), error
    tb["date"] = pd.to_datetime(tb[["year", "month"]].assign(day=15))

    # Add a location column.
    tb["location"] = "World"

    # Remove unnecessary columns.
    tb = tb.drop(columns=["year", "month", "decimal"])

    # Pivot table to have a column for each gas.
    tb = tb.pivot(index=["location", "date"], columns=["gas"], join_column_levels_with="_").rename(
        columns={
            "concentration_ch4": "ch4_concentration",
            "concentration_co2": "co2_concentration",
            "concentration_n2O": "n2o_concentration",
        },
        errors="raise",
    )

    # Add a column with a rolling average for each gas.
    tb = add_rolling_average(
        tb=tb, original_column_names=["co2_concentration", "ch4_concentration", "n2o_concentration"]
    )

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["location", "date"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()
