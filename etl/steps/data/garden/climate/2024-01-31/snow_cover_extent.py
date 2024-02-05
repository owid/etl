"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns to select from the data, and how to rename them.
COLUMNS = {
    "date": "date",
    "phcalc_insitu": "ocean_ph",
}


def add_rolling_average(tb: Table, original_column_name: str) -> Table:
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

    # Create a rolling average with a window of one year, linearly interpolating missing values.
    tb_with_average[f"{original_column_name}_yearly_average"] = (
        tb_with_average[original_column_name]
        .interpolate(method="linear")
        .rolling(365)
        .mean()
        .copy_metadata(tb_with_average[original_column_name])
    )

    # Drop empty rows.
    tb_with_average = tb_with_average.dropna(subset=[original_column_name]).reset_index()

    # Remove rolling average for the first year, given that it is based on incomplete data.
    tb_with_average.loc[
        tb_with_average["date"] < tb_with_average["date"].min() + pd.Timedelta(days=365),
        f"{original_column_name}_yearly_average",
    ] = None

    # Sort conveniently.
    tb_with_average = tb_with_average.sort_values(["location", "date"]).reset_index(drop=True)

    # Check that the values of the original column have not been altered.
    error = f"The values of the original {original_column_name} column have been altered."
    assert tb_with_average[original_column_name].astype(int).equals(tb[original_column_name].astype(int)), error

    return tb_with_average


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its tables.
    ds_meadow = paths.load_dataset("snow_cover_extent")
    tb = ds_meadow["snow_cover_extent"].reset_index()

    #
    # Process data.
    #
    # Create a date column.
    # NOTE: Assign the middle of the month.
    tb["date"] = pd.to_datetime(tb[["year", "month"]].assign(day=15))
    tb = tb.drop(columns=["year", "month"], errors="raise")

    # Data starts in 1966, but, as mentioned on their website
    # https://climate.rutgers.edu/snowcover/table_area.php?ui_set=1&ui_sort=0
    # there is missing data between 1968 and 1971.
    # So, for simplicity, select data from 1972 onwards, where data is complete.
    tb = tb[tb["date"] >= "1972-01-01"].reset_index(drop=True)

    # Add a column with a rolling average.
    tb = add_rolling_average(tb=tb, original_column_name="snow_cover_extent")

    # Set an appropriate index to each table and sort conveniently.
    tb = tb.set_index(["location", "date"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()
