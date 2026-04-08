import datetime as dt

import pandas as pd

from etl.helpers import PathFinder, grapher_checks

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset()

    # Read table from garden dataset.
    tb = ds_garden.read("excess_mortality_economist")

    # Grapher doesn't support dates, only years, but we can convert it to int and pretend it's a year.
    reference_date = dt.datetime.strptime("2020/01/01", "%Y/%m/%d")
    tb["year"] = (pd.to_datetime(tb["date"].astype(str)) - reference_date).dt.days

    tb = tb.drop(columns=["date"]).format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata)  # type: ignore

    #
    # Checks.
    #
    grapher_checks(ds_grapher)

    # Save changes in the new grapher dataset.
    ds_grapher.save()
