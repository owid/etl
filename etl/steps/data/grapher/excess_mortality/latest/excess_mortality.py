"""Load a garden dataset and create a grapher dataset."""

import pandas as pd
from owid.catalog import Dataset, Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden: Dataset = paths.load_dependency("excess_mortality")

    # Read table from garden dataset.
    tb_garden = ds_garden["excess_mortality"]

    #
    # Process data.
    #
    tb_garden = make_grapher_friendly(tb_garden)

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = Dataset.create_empty(dest_dir, ds_garden.metadata)

    # Add table of processed data to the new dataset.
    ds_grapher.add(tb_garden)

    # Save changes in the new grapher dataset.
    ds_grapher.save()


def make_grapher_friendly(df: Table) -> Table:
    # year
    reference_date = pd.Timestamp(2020, 1, 1)
    df["year"] = pd.to_datetime(df["date"])
    df["year"] = (df["year"] - reference_date).dt.days
    # country
    df["country"] = df["entity"]
    # drop unused columns
    df = df.drop(columns=["date", "entity"])
    return df
