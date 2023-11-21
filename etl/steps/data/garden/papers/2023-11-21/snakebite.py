"""Load a meadow dataset and create a garden dataset."""

import re

import numpy as np
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
    ds_meadow = paths.load_dataset("snakebite")

    # Read table from meadow dataset.
    tb = ds_meadow["snakebite"].reset_index()

    #
    # Process data.
    #
    tb = clean_values(tb)

    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    tb = tb.set_index(["country", "year"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def clean_values(tb: Table) -> Table:
    """
    The variable columns are formatted as: value (lower bound, upper bound). This function removes the lower and upper bound values in the brackets and leaves the central value.

    """

    columns_to_clean = [
        "deaths_count",
        "age_standardized_death_rate_per_100000",
        "percent_change_in_deaths_from_1990_to_2019",
        "ylls_count",
        "age_standardized_yll_rate_per_100000",
        "percent_change_in_ylls_from_1990_to_2019",
    ]
    tb[columns_to_clean] = tb[columns_to_clean].mask(tb[columns_to_clean].eq("--"), np.nan)

    tb[columns_to_clean] = tb[columns_to_clean].applymap(clean_text)
    tb[columns_to_clean] = tb[columns_to_clean].astype(float)

    return tb


def clean_text(text):
    text = re.sub(r"\([^)]*\)", "", str(text))  # Remove text within parentheses
    text = re.sub(r"<", "", text)  # Remove '<' symbols
    text = re.sub(r",", "", text)  # Remove commas
    text = re.sub(r"%", "", text)  # Remove percentages
    return text
