"""Load a meadow dataset and create a garden dataset."""

import numpy as np
import pandas as pd
from owid.catalog import Table
from owid.catalog import processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("findex")

    # Read table from meadow dataset.
    tb = ds_meadow.read("findex")

    #
    # Process data.
    #
    # Harmonize country names.
    tb = geo.harmonize_countries(
        tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    # Convert to %
    tb["value"] = tb["value"] * 100

    # Add metadata to the table.
    tb = add_metadata(tb)

    tb = combine_countries(
        tb,
        "Sub-Saharan Africa (WB)",
        "Sub-Saharan Africa (excluding high income) (WB)",
        new_country_name="Sub-Saharan Africa (WB)",
    )

    # Improve table format.
    tb = tb.format(["country", "year"])

    # Calculate the percentage of adults who have both accounts
    tb["both_accounts"] = (
        tb["mobile_money_account__pct_age_15plus"]
        + tb["financial_institution_account__pct_age_15plus"]
        - tb["account__pct_age_15plus"]
    )

    # % only mobile money account
    tb["only_mobile_money_account"] = tb["mobile_money_account__pct_age_15plus"] - tb["both_accounts"]

    # % only financial institution account
    tb["only_financial_institution_account"] = tb["financial_institution_account__pct_age_15plus"] - tb["both_accounts"]

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()


def combine_countries(tb, country1, country2, new_country_name):
    # check if both countries have the same values
    rel_cols = [col for col in tb.columns if col not in ["country"]]
    c1_tb = tb[tb["country"] == country1]
    c2_tb = tb[tb["country"] == country2]
    equal = tables_equal_values(c1_tb[rel_cols], c2_tb[rel_cols])

    if not equal:
        raise ValueError(f"Countries {country1} and {country2} have different values for {rel_cols}.")

    # combine the two countries
    c2_tb.index = c1_tb.index  # align indices
    comb_tb = c1_tb.combine_first(c2_tb)
    comb_tb["country"] = new_country_name

    # remove the old countries
    tb = tb[~tb["country"].isin([country1, country2])]

    # append the combined country
    tb = pr.concat([tb, comb_tb], ignore_index=True)

    return tb


def add_metadata(tb: Table) -> Table:
    """
    Add metadata to the table.
    """
    # Pivot the table to have the indicators as columns to add descriptions from producer and description_short.
    tb_pivoted = tb.pivot(index=["country", "year"], columns="indicator_name", values="value")

    for column in tb_pivoted.columns:
        meta = tb_pivoted[column].metadata
        long_definition = tb["long_definition"].loc[tb["indicator_name"] == column]
        short_definition = tb["short_definition"].loc[tb["indicator_name"] == column]
        meta.description_from_producer = long_definition.iloc[0]
        meta.description_short = short_definition.iloc[0]
        meta.title = column
        meta.unit = "%"
        meta.short_unit = "%"
    tb_pivoted = tb_pivoted.reset_index()
    return tb_pivoted


def tables_equal_values(tb1, tb2) -> bool:
    """
    Check if two tables have the same values at all positions,
    regardless of index or column labels.

    Returns:
        bool: True if all values are the same, False otherwise.
    """
    # First, check shape
    if tb1.shape != tb2.shape:
        print("Tables have different shapes:", tb1.shape, "vs", tb2.shape)
        return False

    a = tb1.to_numpy(dtype="object")
    b = tb2.to_numpy(dtype="object")

    result = np.empty(a.shape, dtype=bool)

    for i in range(a.shape[0]):
        for j in range(a.shape[1]):
            val1 = a[i, j]
            val2 = b[i, j]
            if pd.isna(val1) or pd.isna(val2):
                result[i, j] = True
            else:
                result[i, j] = val1 == val2

    return result.all()
