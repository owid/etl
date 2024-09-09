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
    ds_garden = paths.load_dataset("fluid")

    # Read table from garden dataset.
    tb = ds_garden["fluid"]
    tb["year"] = pd.to_datetime(tb["iso_weekstartdate"], format="%Y-%m-%d", utc=True).dt.date.astype(str)

    tb_test = tb[["country", "date", "inf_tested"]].dropna(subset="inf_tested")
    tb_test = tb_test.format(["country", "year"])
    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb_test], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
