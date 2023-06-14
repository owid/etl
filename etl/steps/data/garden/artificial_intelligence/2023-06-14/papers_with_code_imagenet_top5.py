"""Load a meadow dataset and create a garden dataset."""

from typing import cast

import pandas as pd
import shared
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("papers_with_code_imagenet_top5.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("papers_with_code_imagenet_top5"))
    df = pd.DataFrame(ds_meadow["papers_with_code_imagenet_top5"])

    df["performance"] = df["performance"] * 100
    df["additional_data"] = df["additional_data"].cat.rename_categories(
        {"false": "Without extra training data", "true": "With extra training data"}
    )
    df["date"] = pd.to_datetime(df["date"])

    # Extract the year into a new column
    df["year"] = df["date"].dt.year

    # Drop the original date column
    df.drop("date", axis=1, inplace=True)

    pivot_df = pd.pivot_table(df, values="performance", index=["name", "year"], columns="additional_data")
    pivot_df.reset_index(inplace=True)
    pivot_df.index.name = None
    # Create empty dictionaries to store the models with the highest performance for each year
    df_best = shared.select_best(pivot_df)
    # Convert the "name" column to string type
    combined = shared.combine_with_without(df_best)

    tb = Table(combined, short_name="papers_with_code_imagenet_top5", underscore=True)
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("papers_with_code_imagenet_top5.end")
