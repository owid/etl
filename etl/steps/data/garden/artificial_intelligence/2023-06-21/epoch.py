"""Load a meadow dataset and create a garden dataset."""

from typing import cast

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("epoch.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("epoch"))

    # Read table from meadow dataset.
    tb = ds_meadow["epoch"]
    df = pd.DataFrame(tb)

    # Filter notable systems by selecting rows where 'inclusion_criteria' is not nan
    df = df[df["inclusion_criteria"].notna()].reset_index(drop=True)
    df.drop("inclusion_criteria", axis=1, inplace=True)
    #
    # Process data.
    # Clean up researcher affiliation in column 'organization_categorization'
    category_mapping = {
        "Industry - Academia Collaboration": "Collaboration",
        "Industry - Academia collaboration": "Collaboration",
        "Industry - Academia Collaboration (Academia leaning)": "Collaboration, majority academia",
        "Industry - Academia Collaboration (Academia Leaning)": "Collaboration, majority academia",
        "Industry - Academia Collaboration (Industry Leaning)": "Collaboration, majority industry",
        "Industry - Academia Collaboration (Industry leaning)": "Collaboration, majority industry",
        "Research Collective": "Research collective",
    }
    df["system"] = df["system"].replace({"Univeristy": "University", "Nvidia": "NVIDIA"}, regex=True)
    df["organization_categorization"] = df["organization_categorization"].replace(category_mapping)

    # Convert FLOP to petaFLOP
    df["training_computation_petaflop"] = df["training_compute__flop"] / 1e15
    df["publication_date"] = pd.to_datetime(df["publication_date"])

    # then, calculate 'days_since_1949'
    df["days_since_1949"] = (df["publication_date"] - pd.to_datetime("1949-01-01")).dt.days

    df.dropna(subset=["days_since_1949"], inplace=True)
    df = df.reset_index(drop=True)
    # df.drop("publication_date", axis=1, inplace=True)
    df["days_since_1949"] = df["days_since_1949"].astype(int)

    df = clean_non_unique_year_model(df)
    # There is a typo in the domain name "VIsion"
    df.replace({"VIsion": "Vision"}, inplace=True)

    # Create table
    tb = Table(df, short_name="epoch", underscore=True)
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("epoch.end")


def clean_non_unique_year_model(df):
    """
    Clean the DataFrame by handling non-unique (days since) and model index values.

    Args:
        df (pandas.DataFrame): The input DataFrame.

    Returns:
        pandas.DataFrame: The cleaned DataFrame.
    """

    # Set the index to 'system' and 'days_since_1949'
    df.set_index(["system", "days_since_1949"], inplace=True)
    # Check for non-unique indexes
    non_unique_indexes = df.index[df.index.duplicated()]

    for index in non_unique_indexes:
        # Sort index levels before indexing
        df.sort_index(level=df.index.names, inplace=True)

        # Get rows with the current index
        rows = df.loc[index]

        # Handle values in columns for the current index
        for column in df.columns:
            non_nan_values = rows[column].dropna()  # Check non-NaN values
            if len(non_nan_values) > 0:
                if len(non_nan_values) > 1:
                    # Assert that all non-NaN values are identical
                    assert non_nan_values.nunique() == 1, "Non-identical values found within a column."
                df.loc[index, column] = non_nan_values.iloc[0]  # Set value if not nan
            else:
                df.loc[index, column] = np.nan  # Set NaN if all values are NaN

    # Drop duplicate rows based on index
    df = df[~df.index.duplicated(keep="first")]
    # Check if duplicates still exist in the DataFrame
    duplicates = df.index.duplicated()
    if duplicates.any():
        log.info("Duplicates still exist in the DataFrame.")
    else:
        log.info("No duplicates found in the DataFrame.")

    # Reset the index to restore the original structure
    df.reset_index(inplace=True)

    return df
