"""Load a snapshot and create a meadow dataset."""

import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

VARIABLE_LIST = [
    "Gross domestic product (GDP), Constant prices, Percent change",
    "Unemployment rate",
]

RELEASE_YEAR = int(paths.version.split("-")[0])


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("world_economic_outlook.csv")

    # Load data from snapshot.
    tb = snap.read_csv(low_memory=False)

    #
    # Process data.
    #
    tb = prepare_data(tb)
    tb = reshape_and_clean(tb)

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def prepare_data(tb: Table) -> Table:
    """
    Prepares the data by selecting relevant columns and filtering variables.
    """
    # Select the data we want to import.
    tb = tb[
        ["COUNTRY", "INDICATOR", "LATEST_ACTUAL_ANNUAL_DATA"] + [str(year) for year in tb.columns if year.isdigit()]
    ].dropna(subset=["COUNTRY"])

    # Make column names in lower case.
    tb.columns = tb.columns.str.lower()

    # Select only the variables we want to import.
    tb = tb[tb["indicator"].isin(VARIABLE_LIST)].reset_index(drop=True)

    # Format latest_actual_annual_data as integer.
    # There are some non-integer values like FYYYY/YY, so we want that YY part only, converted to 20YY format.

    tb["latest_actual_annual_data"] = tb["latest_actual_annual_data"].apply(convert_to_year)

    # When the column is empty, assign the release year as the latest actual annual data.
    # This is for IMF regions where this data is not available.
    tb["latest_actual_annual_data"] = tb["latest_actual_annual_data"].fillna(RELEASE_YEAR)

    return tb


def convert_to_year(x):
    if pd.isna(x):
        return pd.NA
    if isinstance(x, str) and x.startswith("FY"):
        return int("20" + x[-2:])
    return int(x)


def reshape_and_clean(tb: Table) -> Table:
    """
    Reshapes the table from wide to long format and cleans the data.
    """

    tb = tb.melt(id_vars=["country", "indicator", "latest_actual_annual_data"], var_name="year")

    # Coerce values to numeric.
    tb["year"] = tb["year"].astype("Int64")
    tb["latest_actual_annual_data"] = tb["latest_actual_annual_data"].astype("Int64")

    # Split between observations and forecasts
    tb.loc[tb.year > tb["latest_actual_annual_data"], "indicator"] += "_forecast"
    tb.loc[tb.year <= tb["latest_actual_annual_data"], "indicator"] += "_observation"

    # Drop rows with missing values.
    tb = tb.dropna(subset=["value"])

    # Drop latest_actual_annual_data
    tb = tb.drop(columns="latest_actual_annual_data")

    tb = tb.pivot(
        index=["country", "year"],
        columns="indicator",
        values="value",
        join_column_levels_with="_",
    )

    return tb
