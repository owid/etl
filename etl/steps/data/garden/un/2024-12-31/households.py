"""Load a meadow dataset and create a garden dataset."""

import numpy as np
import pandas as pd

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
        "average_household_size__number_of_members",
        "female_head_of_household__percentage_of_households",
        "one_person",
        "couple_only",
        "couple_with_children",
        "single_parent_with_children",
        "single_mother_with_children",
        "single_father_with_children",
        "extended_family",
        "non_relatives",
        "unknown",
        "nuclear",
        "multi_generation",
        "three_generation",
        "skip_generation",
    ]

    # Replace ".." with NaN
    tb = tb.replace("..", np.nan)

    # Make sure these columns add up to around 100 for each country and year
    # Some years don't have all the data so we want to make those NaNs to exclude them.
    columns_to_sum = [
        "unknown",
        "non_relatives",
        "extended_family",
        "single_parent_with_children",
        "couple_with_children",
        "couple_only",
        "one_person",
    ]

    # Convert columns to numeric, forcing errors to NaN
    tb[columns_to_sum] = tb[columns_to_sum].apply(pd.to_numeric, errors="coerce")

    # Calculate the sum of the specified columns
    tb["sum_households"] = tb[columns_to_sum].sum(axis=1)

    # Check which rows don't add up to around 100 (using a tolerance, e.g., ±0.5)
    tolerance = 0.5
    rows_not_around_100 = tb[(tb["sum_households"] < 100 - tolerance) | (tb["sum_households"] > 100 + tolerance)]
    # Make the values NaN for the columns in the rows that don't add up to around 100
    tb.loc[rows_not_around_100.index, columns_to_sum] = np.nan

    # Drop the sum_households column if needed
    tb = tb.drop(columns=["sum_households"])
    # Some years don't have all the data so we want to make those NaNs to exclude them.
    columns_for_other = [
        "unknown",
        "single_parent_with_children",
        "couple_with_children",
        "couple_only",
        "one_person",
    ]
    tb["other"] = 100 - tb[columns_for_other].sum(axis=1)

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
