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
    ds_garden = paths.load_dataset("cases_deaths")

    # Read table from garden dataset.
    tb = ds_garden["cases_deaths"].reset_index()

    #
    # Process data.
    #
    tb["year"] = (pd.to_datetime(tb["date"].astype(str), format="%Y-%m-%d") - pd.to_datetime("2020-01-21")).dt.days
    tb = tb.drop(columns=["date"])
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir,
        tables=[tb],
        check_variables_metadata=True,
        default_metadata=ds_garden.metadata,
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
