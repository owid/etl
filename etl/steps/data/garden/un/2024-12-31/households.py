"""Load a meadow dataset and create a garden dataset."""

import numpy as np
import pandas as pd
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("households")

    # Read table from meadow dataset.
    tb = ds_meadow.read("households")

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    columns_to_keep = [
        "one_person",
        "couple_only",
        "couple_with_children",
        "single_parent_with_children",
        "extended_family",
        "non_relatives",
        "unknown",
    ]

    # Replace ".." with NaN
    tb = tb.replace("..", np.nan)

    # Add "other" category to the household types
    tb = create_other_category(tb)

    tb = tb[columns_to_keep + ["country", "year"]]
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def create_other_category(tb: Table) -> Table:
    """
    Ensure that the relevant household columns add up to around 100 for each country and year, and create an 'other' category.

    Parameters:
    tb (Table): The original table containing household data.

    Returns:
    Table: The updated table with the 'other' category added and cleaned data.
    """
    # Make sure these columns add up to around 100 for each country and year
    columns_to_sum = [
        "unknown",
        "non_relatives",
        "extended_family",
        "single_parent_with_children",
        "couple_with_children",
        "couple_only",
        "one_person",
    ]

    # Convert columns to numeric
    tb[columns_to_sum] = tb[columns_to_sum].apply(pd.to_numeric, errors="coerce")

    # Calculate the sum of the specified columns
    tb["sum_households"] = tb[columns_to_sum].sum(axis=1)

    # Check which rows don't add up to around 100 (using a tolerance, e.g., ±0.1)
    tolerance = 0.5
    rows_not_around_100 = tb[(tb["sum_households"] < 100 - tolerance) | (tb["sum_households"] > 100 + tolerance)]

    # Calculate the difference from 100 for the rows that don't add up to around 100 and set it to the "unknown" column
    tb.loc[rows_not_around_100.index, "unknown"] = tb.loc[rows_not_around_100.index, "unknown"].fillna(0) + (
        100 - tb.loc[rows_not_around_100.index, "sum_households"]
    )
    # Ensure if the "unknown" column row is 100, then others are 0
    tb.loc[tb["unknown"] == 100, columns_to_sum] = 0
    tb.loc[tb["unknown"] == 100, "unknown"] = 100

    # Drop the sum_households column if needed
    tb = tb.drop(columns=["sum_households"])

    # Ensure other columns are not NaN for rows where "unknown" is not NaN
    # tb.loc[tb["unknown"].notna(), columns_to_sum] = tb.loc[tb["unknown"].notna(), columns_to_sum].fillna(0)

    # If all values are zero for each of the columns in columns_to_sum, set them to NaN (avoid plotting just 0s)
    tb.loc[(tb[columns_to_sum] == 0).all(axis=1), columns_to_sum] = np.nan

    return tb
