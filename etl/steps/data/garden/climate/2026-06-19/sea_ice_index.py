"""Load a meadow dataset and create a garden dataset."""

import pandas as pd

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("sea_ice_index")

    # Read table from meadow dataset.
    tb = ds_meadow.read("sea_ice_index")

    #
    # Process data.
    #
    # Remove column with annual average.
    tb = tb.drop(columns=["annual"])

    # Convert table to long format.
    tb = tb.melt(id_vars=["location", "year"], var_name="month", value_name="sea_ice_extent")

    # Create column of date, assuming each measurement is taken mid month.
    tb["date"] = pd.to_datetime(tb["year"].astype(str) + tb["month"].str[0:3] + "15", format="%Y%b%d")

    # Drop empty rows and unnecessary columns.
    tb = tb.dropna().drop(columns=["year", "month"])

    # Set an appropriate index and sort conveniently.
    tb = tb.format(["location", "date"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the combined table.
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
