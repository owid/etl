"""Load a garden dataset and create a grapher dataset."""

from typing import Optional

import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

ZERO_DATE = "1995-01-02"


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("flu_yamagata")

    # Read table from garden dataset.
    tb = ds_garden.read("flu_yamagata", reset_index=False)

    tb = to_grapher_date(tb)
    #
    # Save outputs.
    #
    # Initialize a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata)

    # Save grapher dataset.
    ds_grapher.save()


def to_grapher_date(tb: Table, zero_day: Optional[str] = None) -> Table:
    """Modify date so Grapher understands it."""
    if zero_day is None:
        zero_day = ZERO_DATE
    # Get column names for indices
    column_index = tb.index.names
    # Reset index
    tb = tb.reset_index()
    # Get new 'date', drop old date
    tb["year"] = (pd.to_datetime(tb["date"].astype(str), format="%Y-%m-%d") - pd.to_datetime(zero_day)).dt.days
    tb = tb.drop(columns=["date"])
    # Set index again
    column_index = [col if col != "date" else "year" for col in column_index]
    tb = tb.format(column_index)
    return tb
