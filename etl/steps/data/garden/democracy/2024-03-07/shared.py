from typing import Any, Callable, Dict, List, Optional, cast

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
    tb: Table,
    ds_population: Dataset,
    expected_countries_without_population: Optional[List[str]] = None,
    drop_population: bool = True,
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
    if drop_population:
        tb = tb.drop(columns="population")
    ## Add metadata back (combine origins from population)
    for col in cols:
        metadata = meta[col]
        metadata.origins += meta["population"].origins
        tb[col].metadata = meta[col]

    return tb


def make_table_with_dummies(tb: Table, indicators: List[Dict[str, Any]]) -> Table:
    """Format table to have dummy indicators.

    From a table with categorical indicators, create a new table with dummy indicator for each indicator-category pair.

    Example input:

    | year | country |  regime   | regime_amb |
    |------|---------|-----------|------------|
    | 2000 |   USA   |     1     |      0     |
    | 2000 |   CAN   |     0     |      1     |
    | 2000 |   DEU   |    NaN    |      NaN   |


    Example output:

    | year | country | regime_0 | regime_1 | regime_-1 | regime_amb_0 | regime_amb_0 | regime_amb_-1 |
    |------|---------|----------|----------|-----------|--------------|--------------|---------------|
    | 2000 |   USA   |    0     |    1     |     0     |      1       |      0       |       0       |
    | 2000 |   CAN   |    1     |    0     |     0     |      0       |      1       |       0       |
    | 2000 |   DEU   |    0     |    0     |     1     |      0       |      0       |       1       |

    Note that '-1' denotes NA (missing value) category.

    The argument `indicators` contains the indicators for which we will create dummies, along with other associated paramters. Example:

    {
        "name": "regime_amb_row_owid",
        "name_new": "num_countries_regime_amb",
        # "values_expected": set(map(str, range(10))),
        "values_expected": {
            "0": "closed autocracy",
            "1": "closed (maybe electoral) autocracy",
            "2": "electoral (maybe closed) autocracy",
            "3": "electoral autocracy",
            "4": "electoral autocracy (maybe electoral democracy)",
            "5": "electoral democracy (maybe electoral autocracy)",
            "6": "electoral democracy",
            "7": "electoral democracy (maybe liberal democracy)",
            "8": "liberal democracy (maybe electoral democracy)",
            "9": "liberal democracy",
        },
        "has_na": True,
    }
    """
    tb_ = tb.copy()

    # Convert to string
    indicator_names = [indicator["name"] for indicator in indicators]
    tb_[indicator_names] = tb_[indicator_names].astype("string")

    # Sanity check that the categories for each indicator are as expected
    for indicator in indicators:
        values_expected = indicator["values_expected"]
        # Check and fix NA (convert NAs to -1 category)
        if indicator["has_na"]:
            # Assert that there are actually NaNs
            assert tb_[indicator["name"]].isna().any(), "No NA found!"
            # If NA, we should not have category '-1', otherwise these would get merged!
            assert "-1" not in set(
                tb_[indicator["name"]].unique()
            ), f"Error for indicator `{indicator['name']}`. Found -1, which is not allowed when `has_na=True`!"
            tb_[indicator["name"]] = tb_[indicator["name"]].fillna("-1")
            # Add '-1' as a possible category
            if isinstance(values_expected, dict):
                indicator["values_expected"]["-1"] = "-1"
            else:
                values_expected |= {"-1"}
        else:
            assert not tb_[indicator["name"]].isna().any(), "NA found!"

        values_found = set(tb_[indicator["name"]].unique())
        assert values_found == set(
            values_expected
        ), f"Error for indicator `{indicator['name']}`. Expected {set(values_expected)} but found {values_found}"

        # Rename dimension values
        if isinstance(values_expected, dict):
            tb_[indicator["name"]] = tb_[indicator["name"]].map(indicator["values_expected"])

    ## Rename columns
    tb_ = tb_.rename(columns={indicator["name"]: indicator["name_new"] for indicator in indicators})
    indicator_names = [indicator["name_new"] for indicator in indicators]

    ## Get dummy indicator table
    tb_ = cast(Table, pd.get_dummies(tb_, dummy_na=True, columns=indicator_names, dtype=int))

    ## Add missing metadata to dummy indicators
    dummy_cols = []
    for indicator in indicators:
        ## get list of dummy indicator column names
        if isinstance(indicator["values_expected"], dict):
            dummy_columns = [f"{indicator['name_new']}_{v}" for v in indicator["values_expected"].values()]
        else:
            dummy_columns = [f"{indicator['name_new']}_{v}" for v in indicator["values_expected"]]
        ## assign metadata to dummy column indicators
        for col in dummy_columns:
            tb_[col].metadata = tb[indicator["name"]].metadata
        dummy_cols.extend(dummy_columns)

    ### Select subset of columns
    tb_ = tb_.loc[:, ["year", "country"] + dummy_cols]

    return tb_
