from typing import Callable, List, Optional, cast

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table

from etl.data_helpers import geo


def from_wide_to_long(
    tb: Table,
    indicator_name_callback: Optional[Callable] = None,
    indicator_category_callback: Optional[Callable] = None,
    column_dimension_name: str = "category",
) -> Table:
    """Format a particular shape of table from wide to long format.

    tb: Table with wide format.
    indicator_name_callback: Function to extract the indicator name from the column name.
    indicator_category_callback: Function to extract the indicator category from the column name.

    If no `indicator_name_callback` and `indicator_category_callback` are provided, it proceed expects the following input:

    | year | country | indicator_a_1 | indicator_a_2 | indicator_b_1 | indicator_b_2 |
    |------|---------|---------------|---------------|---------------|---------------|
    | 2000 |   USA   |       1       |       2       |       3       |       4       |
    | 2000 |   CAN   |       5       |       6       |       7       |       8       |

    and then generates the output:

    | year | country |  category  | indicator_a | indicator_b |
    |------|---------|------------|-------------|-------------|
    | 2000 | USA     | category_1 |      1      |       3     |
    | 2000 | USA     | category_2 |      2      |       4     |
    """
    # Melt the DataFrame to long format
    tb = tb.melt(id_vars=["year", "country"], var_name="indicator_type", value_name="value")

    # Get callables
    if indicator_name_callback is None:

        def default_indicator_name(x):
            return "_".join(x.split("_")[:-1])

        indicator_name_callback = default_indicator_name

    if indicator_category_callback is None:

        def default_indicator_category(x):
            return x.split("_")[-1]

        indicator_category_callback = default_indicator_category

    # Extract indicator names and types
    tb["indicator"] = tb["indicator_type"].apply(indicator_name_callback)
    tb[column_dimension_name] = tb["indicator_type"].apply(indicator_category_callback)

    # Drop the original 'indicator_type' column as it's no longer needed
    tb.drop("indicator_type", axis=1, inplace=True)

    # Pivot the table to get 'indicator_a' and 'indicator_b' as separate columns
    tb = tb.pivot(index=["year", "country", column_dimension_name], columns="indicator", values="value").reset_index()

    # Rename the columns to match your requirements
    tb.columns.name = None  # Remove the hierarchy

    return tb


def expand_observations(tb: Table) -> Table:
    """Expand to have a row per (year, country)."""
    # Add missing years for each triplet ("warcode", "campcode", "ccode")

    # List of countries
    regions = set(tb["country"])

    # List of possible years
    years = np.arange(tb["year"].min(), tb["year"].max() + 1)

    # New index
    new_idx = pd.MultiIndex.from_product([years, regions], names=["year", "country"])

    # Reset index
    tb = tb.set_index(["year", "country"]).reindex(new_idx).reset_index()

    # Type of `year`
    tb["year"] = tb["year"].astype("int")
    return tb


def add_population_in_dummies(
    tb: Table, ds_population: Dataset, expected_countries_without_population: Optional[List[str]] = None
):
    # Add population column
    tb = geo.add_population_to_table(
        tb,
        ds_population,
        interpolate_missing_population=True,
        expected_countries_without_population=expected_countries_without_population,
    )
    tb = cast(Table, tb.dropna(subset="population"))
    # Add metadata (origins combined indicator+population)
    cols = [col for col in tb.columns if col not in ["year", "country", "population"]]
    meta = {col: tb[col].metadata for col in cols} | {"population": tb["population"].metadata}
    ## Encode population in indicators: Population if 1, 0 otherwise
    tb[cols] = tb[cols].multiply(tb["population"], axis=0)
    tb = tb.drop(columns="population")
    ## Add metadata back (combine origins from population)
    for col in cols:
        metadata = meta[col]
        metadata.origins += meta["population"].origins
        tb[col].metadata = meta[col]

    return tb
