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
    ds_garden = paths.load_dataset("surface_land_temperature")
    tb = ds_garden["surface_land_temperature"].reset_index()

    #
    # Process data.
    #

    # Calculate the number of days since 1949
    tb["days_since_1941"] = (tb["time"] - pd.to_datetime("1949-01-01")).dt.days

    tb = tb.drop(columns=["time"])
    tb.rename(columns={"days_since_1941": "year"}, inplace=True)
    tb = tb.set_index(["country", "year"])
    for column in tb.columns:
        tb[column].metadata.display = {}
        tb[column].metadata.display["zeroDay"] = "1949-01-01"
        tb[column].metadata.display["yearIsDay"] = True

    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb], default_metadata=ds_garden.metadata)
    ds_grapher.metadata.title = "Surface land temperatures and anomalies since 1950 by country"

    ds_grapher.save()
