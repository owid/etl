"""Load a snapshot and create a meadow dataset."""

import numpy as np
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# This is a preliminary bulk importer for the IMF's WEO dataset.
# As of October 2021, it only generates a grapher-compatible dataset with 1 variable (GDP growth).
# But this first version could be extended to a traditional bulk import of the entire dataset later.
VARIABLE_LIST = [
    "NGDP_RPCH",  # Gross domestic product, constant prices / Percent change
    "LUR",  # Unemployment rate / Percent of total labor force
]


def select_data(tb: Table) -> Table:
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
    tb["variable"] = tb["Subject Descriptor"] + " - " + tb["Units"]
    tb = tb.drop(columns=["Subject Descriptor", "Units"])
    return tb


def pick_variables(tb: Table) -> Table:
    return tb[tb["WEO Subject Code"].isin(VARIABLE_LIST)].drop(columns="WEO Subject Code")


def reshape_and_clean(tb: Table) -> Table:
    tb = tb.melt(id_vars=["Country", "variable", "Estimates Start After"], var_name="year")

    # Coerce values to numeric.
    tb["value"] = tb["value"].replace("--", np.nan).astype(float)
    tb["year"] = tb["year"].astype(int)

    # Split between observations and forecasts
    tb.loc[tb.year > tb["Estimates Start After"], "variable"] += "_forecast"
    tb.loc[tb.year <= tb["Estimates Start After"], "variable"] += "_observation"

    # Drop rows with missing values.
    tb = tb.dropna(subset=["value"])

    # Drop Estimates Start After
    tb = tb.drop(columns="Estimates Start After")

    tb = tb.pivot(index=["Country", "year"], columns="variable", values="value", join_column_levels_with="_")
    return tb


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("world_economic_outlook.xls")

    # Load data and metadata from snapshot.
    tb = snap.read_csv(delimiter="\t", encoding="ISO-8859-1")

    #
    # Process data.
    #
    # Prepare raw data.
    tb = select_data(tb).pipe(make_variable_names).pipe(pick_variables).pipe(reshape_and_clean)

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.underscore().set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir,
        tables=[tb],
        check_variables_metadata=True,
        default_metadata=snap.metadata,
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()
