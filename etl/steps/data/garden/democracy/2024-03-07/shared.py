from typing import Any, Callable, Dict, List, Optional, Tuple, cast

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.tables import concat

from etl.data_helpers import geo

SEPARATOR = "."
# REGION AGGREGATES
REGIONS = {
    "Africa": {
        "additional_members": [
            "Somaliland",
            "Zanzibar",
        ]
    },
    "Asia": {
        "additional_members": [
            "Palestine/Gaza",
            "Palestine/West Bank",
        ]
    },
    "North America": {},
    "South America": {},
    "Europe": {
        "additional_members": [
            "Baden",
            "Bavaria",
            "Brunswick",
            "Duchy of Nassau",
            "Hamburg",
            "Hanover",
            "Hesse Electoral",
            "Hesse Grand Ducal",
            "Mecklenburg Schwerin",
            "Modena",
            "Oldenburg",
            "Parma",
            "Piedmont-Sardinia",
            "Saxe-Weimar-Eisenach",
            "Saxony",
            "Tuscany",
            "Two Sicilies",
            "Wurttemberg",
        ]
    },
    "Oceania": {},
}


def from_wide_to_long(
    tb: Table,
    indicator_name_callback: Optional[Callable] = None,
    indicator_category_callback: Optional[Callable] = None,
    column_dimension_name: str = "category",
    separator: str = SEPARATOR,
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
    tb_ = tb.copy()

    # Melt the DataFrame to long format
    tb_ = tb_.melt(id_vars=["year", "country"], var_name="indicator_type", value_name="value")

    # Get callables
    if indicator_name_callback is None:

        def default_indicator_name(x):
            assert len(x.split(separator)) == 2
            return x.split(separator)[0]

        indicator_name_callback = default_indicator_name

    if indicator_category_callback is None:

        def default_indicator_category(x):
            assert len(x.split(separator)) == 2
            return x.split(separator)[-1]

        indicator_category_callback = default_indicator_category

    # Extract indicator names and types
    tb_["indicator"] = tb_["indicator_type"].apply(indicator_name_callback)
    tb_[column_dimension_name] = tb_["indicator_type"].apply(indicator_category_callback)

    # Drop the original 'indicator_type' column as it's no longer needed
    tb_.drop("indicator_type", axis=1, inplace=True)

    # Pivot the table to get 'indicator_a' and 'indicator_b' as separate columns
    tb_ = tb_.pivot(index=["year", "country", column_dimension_name], columns="indicator", values="value").reset_index()

    # Rename the columns to match your requirements
    tb_.columns.name = None  # Remove the hierarchy

    return tb_


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


def make_table_with_dummies(
    tb: Table,
    indicators: List[Dict[str, Any]],
    separator: str = SEPARATOR,
) -> Table:
    """Format table to have dummy indicators.

    From a table with categorical indicators, create a new table with dummy indicator for each indicator-category pair.

    Example input:

    | year | country |  regime   | regime_amb |
    |------|---------|-----------|------------|
    | 2000 |   USA   |     1     |      0     |
    | 2000 |   CAN   |     0     |      1     |
    | 2000 |   DEU   |    NaN    |      NaN   |


    Example output:

    | year | country | regime.0 | regime.1 | regime.-1 | regime_amb.0 | regime_amb.0 | regime_amb.-1 |
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
            assert tb_[indicator["name"]].isna().any(), f"No NA found in {indicator['name']}!"
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
            assert not tb_[indicator["name"]].isna().any(), f"NA found in {indicator['name']}!"

        values_found = set(tb_[indicator["name"]].unique())
        assert values_found == set(
            values_expected
        ), f"Error for indicator `{indicator['name']}`. Expected {set(values_expected)} but found {values_found}"

        # Rename dimension values
        if isinstance(values_expected, dict):
            tb_[indicator["name"]] = tb_[indicator["name"]].map(indicator["values_expected"])

    ## Rename columns
    tb_ = tb_.rename(
        columns={indicator["name"]: indicator.get("name_new", indicator["name"]) for indicator in indicators}
    )
    indicator_names = [indicator.get("name_new", indicator["name"]) for indicator in indicators]

    ## Get dummy indicator table
    tb_ = cast(Table, pd.get_dummies(tb_, dummy_na=True, columns=indicator_names, dtype=int, prefix_sep=separator))

    ## Add missing metadata to dummy indicators
    dummy_cols = []
    for indicator in indicators:
        name_new = indicator.get("name_new", indicator["name"])
        ## get list of dummy indicator column names
        if isinstance(indicator["values_expected"], dict):
            dummy_columns = [f"{name_new}{separator}{v}" for v in indicator["values_expected"].values()]
        else:
            dummy_columns = [f"{name_new}{separator}{v}" for v in indicator["values_expected"]]
        ## assign metadata to dummy column indicators
        for col in dummy_columns:
            tb_[col].metadata = tb[indicator["name"]].metadata
        dummy_cols.extend(dummy_columns)

    ### Select subset of columns
    tb_ = tb_.loc[:, ["year", "country"] + dummy_cols]

    return tb_


def add_regions_and_global_aggregates(
    tb: Table,
    ds_regions: Dataset,
    regions: Optional[Dict[str, Any]] = None,
    aggregations: Optional[Dict[str, str]] = None,
    min_num_values_per_year: Optional[int] = None,
    aggregations_world: Optional[Dict[str, str]] = None,
    short_name: str = "region_counts",
) -> Table:
    """Add regions, and world aggregates."""
    # Copy
    tb_ = tb.copy()

    # Regions considered
    if regions is None:
        regions = REGIONS

    # Add regions
    tb_regions = geo.add_regions_to_table(
        tb_.copy(),
        ds_regions,
        regions=regions,
        aggregations=aggregations,
        min_num_values_per_year=min_num_values_per_year,
    )
    tb_regions = tb_regions.loc[tb_regions["country"].isin(regions.keys())]

    # Add world
    if aggregations_world is None:
        tb_world = tb.groupby("year", as_index=False).sum(numeric_only=True, min_count=1).assign(country="World")
    else:
        tb_world = tb.groupby("year", as_index=False).agg(aggregations_world).assign(country="World")
    tb = concat([tb_regions, tb_world], ignore_index=True, short_name="region_counts")

    return tb


def add_count_years_in_regime(
    tb: Table,
    columns: List[Tuple[str, str, int]],
) -> Table:
    """Add years in a certain regime.

    Two types of counters are generated:
        - Age: Number of years consecutively with a certain regime type.
        - Experience: Number of years with a certain regime type.
    """

    def _count_years_in_regime(tb, col, col_new, th):
        col_th = "thresholded"

        tb[col_th] = pd.cut(tb[col], bins=[-float("inf"), th, float("inf")], labels=[0, 1]).astype(int)
        # Add age of democracy
        tb[f"age_{col_new}"] = tb.groupby(["country", tb[col_th].fillna(0).eq(0).cumsum()])[col_th].cumsum().astype(int)
        tb[f"age_{col_new}"] = tb[f"age_{col_new}"].copy_metadata(tb[col])
        # Add experience with democracy
        tb[f"experience_{col_new}"] = tb.groupby("country")[col_th].cumsum().astype(int)
        tb[f"experience_{col_new}"] = tb[f"age_{col_new}"].copy_metadata(tb[col])
        # Sanity check
        assert (tb.loc[tb[col_th] == 1, f"age_{col_new}"] != 0).all(), "Negative age found!"
        assert (tb.loc[tb[col_th] == 1, f"experience_{col_new}"] != 0).all(), "Negative age found!"
        # Drop unused columns
        tb = tb.drop(columns=[col_th])
        return tb

    if columns:
        for col in columns:
            assert len(col) == 3, "Columns should be a list of tuples with 3 elements: (colname, col_newname, col_th)"
            tb = _count_years_in_regime(tb, *col)
    return tb


def add_age_groups(
    tb: Table,
    column: str,
    column_raw: str,
    threshold: int,
    category_names: Dict[Any, str],
    age_bins: List[int | float] | None = None,
) -> Table:
    """Create category for `column`."""
    column_new = f"group_{column}"

    if age_bins is None:
        age_bins = [0, 18, 30, 60, 90, float("inf")]

    # Create age group labels
    assert len(age_bins) > 1, "There should be at least two age groups."
    labels = []
    for i in range(len(age_bins) - 1):
        labels.append(f"{age_bins[i]}-{age_bins[i+1]} years".replace("-inf", "+"))

    # Create variable for age group of electoral demcoracies
    tb[column_new] = pd.cut(
        tb[column],
        bins=age_bins,
        labels=labels,
    ).astype("string")

    # Add additional categories
    for regime_id, regime_name in category_names.items():
        if regime_id > threshold:
            break
        tb.loc[(tb[column_raw] == regime_id) & tb[column_new].isna(), column_new] = regime_name

    # Copy metadata
    tb[column_new] = tb[column_new].copy_metadata(tb[column])
    return tb
