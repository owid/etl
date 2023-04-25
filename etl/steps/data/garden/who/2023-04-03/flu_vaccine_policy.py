"""Load a meadow dataset and create a garden dataset."""
import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("flu_vaccine_policy.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("flu_vaccine_policy")

    # Read table from meadow dataset.
    tb_meadow = ds_meadow["flu_vaccine_policy"]

    # Create a dataframe with data from the table.
    df = pd.DataFrame(tb_meadow).reset_index()

    #
    # Process data.
    #
    log.info("flu_vaccine_policy.harmonize_countries")
    df = geo.harmonize_countries(
        df=df, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    # Replacing value codes with either missing data or a more descriptive value
    df = df.replace({"ND": np.NaN, "NR": "Not relevant", "Unknown": np.NaN})
    # Removing strings from some values e.g. commas in numbers but not full-stops
    df["how_many_doses_of_influenza_vaccine_were_distributed"] = df[
        "how_many_doses_of_influenza_vaccine_were_distributed"
    ].str.replace(r"[^0-9\.]", "", regex=True)

    df = clean_binary_colums(df)
    df = clean_hemisphere_formulation(df)
    df = remove_erroneous_zeros(df)

    # Create a new table with the processed data.
    df = df.set_index(["country", "year"], verify_integrity=True)
    tb_garden = Table(df, like=tb_meadow)
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("flu_vaccine_policy.end")


def clean_binary_colums(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean columns where the only desired outputs should be 'Yes', 'No' or 'Not relevant'
    """
    # Select out the columns that start with 'is', 'are', 'were' or 'does
    binary_cols = df.columns[df.columns.str.startswith(("is", "are", "were"))]
    dict_map = {"Yes": "Yes", "No": "No"}
    df[binary_cols] = df[binary_cols].applymap(dict_map.get).fillna(np.NaN)

    return df


def clean_hemisphere_formulation(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure consistency in the terms used to describe the hemisphere formulations
    """

    df["what_vaccine_formulation_is_used"] = df["what_vaccine_formulation_is_used"].replace(
        {"Northern hemisphere": "Northern Hemisphere", "Hemisferio Sur": "Southern Hemisphere"}
    )

    assert all(
        df["what_vaccine_formulation_is_used"].isin(
            [np.NaN, "Not relevant", "Both", "Northern Hemisphere", "Southern Hemisphere"]
        )
    )

    return df


def remove_erroneous_zeros(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove the handful of zeros that are found in columns where it is not clear what they mean.
    """
    cols = df.columns.drop("how_many_doses_of_influenza_vaccine_were_distributed")
    df[cols] = df[cols].replace(0, np.NaN)

    return df
