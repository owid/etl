"""Load a garden dataset and create a grapher dataset."""

from typing import Optional

import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

ZERO_DATE = "2020-01-21"


def run(dest_dir: str, paths: PathFinder) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset()

    # Read table from garden dataset.
    tb = ds_garden[paths.short_name]

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb], default_metadata=ds_garden.metadata)

    # Save changes in the new grapher dataset.
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
