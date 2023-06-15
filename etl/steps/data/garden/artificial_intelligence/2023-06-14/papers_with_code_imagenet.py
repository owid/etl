"""Load a meadow dataset and create a garden dataset."""

from typing import cast

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("papers_with_code_imagenet.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("papers_with_code_imagenet"))
    df_top5_top1 = []
    for table_title in ["papers_with_code_imagenet_top1", "papers_with_code_imagenet_top5"]:
        df = pd.DataFrame(ds_meadow[table_title])
        df.rename(columns={"performance": table_title}, inplace=True)
        df[table_title] = df[table_title] * 100
        df["additional_data"] = df["additional_data"].cat.rename_categories(
            {"false": "Without extra training data", "true": "With extra training data"}
        )
        # Calculate the number of days since 2019
        df["date"] = pd.to_datetime(df["date"])

        # Extract the year into a new column
        df["year"] = df["date"].dt.year
        df.drop("date", axis=1, inplace=True)

        # Drop the original date column
        pivot_df = pd.pivot_table(df, values=table_title, index=["name", "year"], columns="additional_data")
        pivot_df.reset_index(inplace=True)
        pivot_df.index.name = None
        df_best = select_best(pivot_df)
        combined = combine_with_without(df_best, table_title)
        df_top5_top1.append(combined)

    merge_top1_top5 = pd.merge(df_top5_top1[0], df_top5_top1[1], on=["year", "name"], how="inner")
    tb = Table(merge_top1_top5, short_name="papers_with_code_imagenet", underscore=True)
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("papers_with_code_imagenet.end")


def select_best(df):
    # Create empty dictionaries to store the models with the highest performance and their values for each year
    max_without_extra_training = {}
    max_with_extra_training = {}

    # Loop through each year
    for year in df["year"].unique():
        # Filter the DataFrame for the current year
        year_data = df[df["year"] == year]

        # Check if there is at least one non-NaN value in "Without extra training data" for the current year
        if not year_data["Without extra training data"].isnull().all():
            # Find the model with the highest performance and its value for "Without extra training data"
            max_without_extra_training[year] = (
                year,
                year_data.loc[year_data["Without extra training data"].idxmax(), "name"],
                year_data.loc[year_data["Without extra training data"].idxmax(), "Without extra training data"],
            )

        # Check if there is at least one non-NaN value in "With extra training data" for the current year
        if not year_data["With extra training data"].isnull().all():
            # Find the model with the highest performance and its value for "With extra training data"
            max_with_extra_training[year] = (
                year,
                year_data.loc[year_data["With extra training data"].idxmax(), "name"],
                year_data.loc[year_data["With extra training data"].idxmax(), "With extra training data"],
            )

    # Create a new DataFrame to store the models with the highest performance and their values for each year
    result_df_with = pd.DataFrame(
        max_without_extra_training.values(), columns=["year", "name", "Without extra training data"]
    )

    # Create a new DataFrame to store the models with the highest performance and their values for each year
    result_df_without = pd.DataFrame(
        max_with_extra_training.values(), columns=["year", "name", "With extra training data"]
    )
    merged_df = pd.merge(result_df_with, result_df_without, on=["year", "name"], how="outer")

    return merged_df


def combine_with_without(df, table_title):
    df["name"] = df["name"].astype(str)
    # Add a star at the end of the "name" column where "without_extra_training_data" is NaN
    df.loc[df["Without extra training data"].isnull(), "name"] += "*"
    df["name"] = df["name"].str.replace(" ", "")

    # Create a copy of the DataFrame
    result_df = df.copy()

    # Combine "without_extra_training_data" and "with_extra_training_data" into one column
    result_df[table_title] = result_df["Without extra training data"].fillna(result_df["With extra training data"])

    # Create a new column indicating whether it's training data or not
    result_df["training_data_" + table_title] = (
        result_df["Without extra training data"]
        .notnull()
        .map({True: "With extra training data", False: "Without extra training data"})
    )

    # Remove the "without_extra_training_data" and "with_extra_training_data" columns
    result_df = result_df.drop(columns=["Without extra training data", "With extra training data"])

    return result_df
