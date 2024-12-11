"""Load a snapshot and create a meadow dataset."""

import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

VARIABLE_LIST = [
    "NGDP_RPCH",  # Gross domestic product, constant prices / Percent change
    "LUR",  # Unemployment rate / Percent of total labor force
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("world_economic_outlook.xls")

    # Load data from snapshot.
    tb = snap.read_csv(delimiter="\t", encoding="utf-16-le")

    print(tb)

    #
    # Process data.
    #
    tb = select_data(tb)
    tb = make_variable_names(tb)
    tb = pick_variables(tb)
    tb = reshape_and_clean(tb)

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def select_data(tb: Table) -> Table:
    """
    Selects the data we want to import from the raw table
    """

    tb = tb.drop(
        columns=[
            "WEO Country Code",
            "ISO",
            "Country/Series-specific Notes",
            "Subject Notes",
            "Scale",
        ]
    ).dropna(subset=["Country"])

    return tb


def make_variable_names(tb: Table) -> Table:
    """
    Creates a variable name from the Subject Descriptor and Units columns.
    """

    tb["variable"] = tb["Subject Descriptor"] + " - " + tb["Units"]
    tb = tb.drop(columns=["Subject Descriptor", "Units"])

    return tb


def pick_variables(tb: Table) -> Table:
    """
    Selects the variables we want to import from the raw table.
    """

    # Select only the variables we want to import.
    tb = tb[tb["WEO Subject Code"].isin(VARIABLE_LIST)].reset_index(drop=True)

    # Drop WEO Subject Code
    tb = tb.drop(columns="WEO Subject Code")

    return tb


def reshape_and_clean(tb: Table) -> Table:
    """
    Reshapes the table from wide to long format and cleans the data.
    """

    # Drop any column with "Unnamed" in the name.
    tb = tb.drop(columns=tb.columns[tb.columns.str.contains("Unnamed")])

    tb = tb.melt(id_vars=["Country", "variable", "Estimates Start After"], var_name="year")

    # Coerce values to numeric.
    tb["value"] = tb["value"].replace("--", pd.NA).astype("Float64")
    tb["year"] = tb["year"].astype("Int64")

    # Split between observations and forecasts
    tb.loc[tb.year > tb["Estimates Start After"], "variable"] += "_forecast"
    tb.loc[tb.year <= tb["Estimates Start After"], "variable"] += "_observation"

    # Drop rows with missing values.
    tb = tb.dropna(subset=["value"])

    # Drop Estimates Start After
    tb = tb.drop(columns="Estimates Start After")

    tb = tb.pivot(
        index=["Country", "year"],
        columns="variable",
        values="value",
        join_column_levels_with="_",
    )

    return tb
