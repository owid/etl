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
    ds_garden = paths.load_dataset("sst")

    # Read table from garden dataset.
    tb = ds_garden.read("sst")

    # Name month column "country" foro grapher purposes

    # Combine month and year into a single column
    tb["date"] = pd.to_datetime(tb["year"].astype(str) + "-" + tb["month"].astype(str) + "-01")
    tb["date"] = tb["date"] + pd.offsets.Day(14)
    tb["days_since_1941"] = (tb["date"] - pd.to_datetime("1949-01-01")).dt.days

    # Drop the original year and month columns
    tb = tb.drop(columns=["year", "month", "date"])

    # Rename the date column to year for grapher purposes
    tb = tb.rename(columns={"days_since_1941": "year"})

    tb = tb.format(["year", "country"])
    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
