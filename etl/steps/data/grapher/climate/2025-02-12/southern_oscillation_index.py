"""Load a garden dataset and create a grapher dataset."""

import pandas as pd

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("southern_oscillation_index")

    # Read table from garden dataset.
    tb = ds_garden.read("southern_oscillation_index")
    tb = tb.drop(columns={"year", "annual"})
    # Drop rows with NaN values
    tb = tb.dropna()

    # Calculate the number of days since 1949
    tb["days_since_1941"] = (tb["date"] - pd.to_datetime("1949-01-01")).dt.days
    tb = tb.rename(columns={"days_since_1941": "year"})
    tb = tb.drop(columns=["date"])

    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
