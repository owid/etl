from typing import List, Optional, Type

import numpy as np
import pandas as pd
from owid.catalog import Table
from typing_extensions import Self


def expand_observations(
    tb: Table, col_year_start: str, col_year_end: str, cols_scale: Optional[List[str]] = None, rounding: bool = True
) -> Table:
    """Expand to have a row per (year, conflict).

    Example
    -------

        Input:

        | dispnum | year_start | year_end |
        |---------|------------|----------|
        | 1       | 1990       | 1993     |

        Output:

        |  year | warcode |
        |-------|---------|
        |  1990 |    1    |
        |  1991 |    1    |
        |  1992 |    1    |
        |  1993 |    1    |

    Parameters
    ----------
    tb : Table
        Original table, where each row is a conflict with its start and end year.

    Returns
    -------
    Table
        Here, each conflict has as many rows as years of activity. Its deaths have been uniformly distributed among the years of activity.
    """
    # For that we scale the number of deaths proportional to the duration of the conflict.
    if cols_scale:
        for col in cols_scale:
            tb[col] = (tb[col] / (tb[col_year_end] - tb[col_year_start] + 1)).copy_metadata(tb[col])
            if rounding:
                tb[col] = tb[col].round()

    # Add missing years for each triplet ("warcode", "campcode", "ccode")
    YEAR_MIN = tb[col_year_start].min()
    YEAR_MAX = tb[col_year_end].max()
    tb_all_years = Table(pd.RangeIndex(YEAR_MIN, YEAR_MAX + 1), columns=["year"])
    tb = tb.merge(tb_all_years, how="cross")
    # Filter only entries that actually existed
    tb = tb[(tb["year"] >= tb[col_year_start]) & (tb["year"] <= tb[col_year_end])]

    return tb


def add_indicators_extra(
    tb: Table,
    tb_regions: Table,
    columns_conflict_rate: Optional[List[str]] = None,
    columns_conflict_mortality: Optional[List[str]] = None,
) -> Table:
    """Scale original columns to obtain new indicators (conflict rate and conflict mortality indicators).

    CONFLICT RATE:
        Scale columns `columns_conflict_rate` based on the number of countries (and country-pairs) in each region and year.

        For each indicator listed in `columns_to_scale`, two new columns are added to the table:
        - `{indicator}_per_country`: the indicator value divided by the number of countries in the region and year.
        - `{indicator}_per_country_pair`: the indicator value divided by the number of country-pairs in the region and year.

    CONFLICT MORTALITY:
        Scale columns `columns_conflict_mortality` based on the population in each region.

        For each indicator listed in `columns_to_scale`, a new column is added to the table:
        - `{indicator}_per_capita`: the indicator value divided by the number of countries in the region and year.


    tb: Main table
    tb_regions: Table with three columns: "year", "region", "num_countries". Gives the number of countries per region per year.
    columns_to_scale: List with the names of the columns that need scaling. E.g. number_ongiong_conflicts -> number_ongiong_conflicts_per_country
    """
    tb_regions_ = tb_regions.copy()

    # Sanity check 1: columns as expected in tb_regions
    assert set(tb_regions_.columns) == {
        "year",
        "region",
        "number_countries",
        "population",
    }, f"Invalid columns in tb_regions {tb_regions_.columns}"
    # Sanity check 2: regions equivalent in both tables
    regions_main = set(tb["region"])
    regions_aux = set(tb_regions_["region"])
    assert regions_main == regions_aux, f"Regions in main table and tb_regions differ: {regions_main} vs {regions_aux}"

    # Ensure full precision
    tb_regions_["number_countries"] = tb_regions_["number_countries"].astype(float)
    tb_regions_["population"] = tb_regions_["population"]  # .astype(float)
    # Get number of country-pairs
    tb_regions_["number_country_pairs"] = (
        tb_regions_["number_countries"] * (tb_regions_["number_countries"] - 1) / 2
    ).astype(int)

    # Add number of countries and number of country pairs to main table
    tb = tb.merge(tb_regions_, on=["year", "region"], how="left")

    if not columns_conflict_rate and not columns_conflict_mortality:
        raise ValueError(
            "Call to function is useless. Either provide `columns_conflict_rate` or `columns_conflict_mortality`."
        )

    # CONFLICT RATES ###########
    if columns_conflict_rate:
        # Add normalised indicators
        for column_name in columns_conflict_rate:
            # Add per country indicator
            column_name_new = f"{column_name}_per_country"
            tb[column_name_new] = (tb[column_name].astype(float) / tb["number_countries"].astype(float)).replace(
                [np.inf, -np.inf], np.nan
            )
            # Add per country-pair indicator
            column_name_new = f"{column_name}_per_country_pair"
            tb[column_name_new] = (tb[column_name].astype(float) / tb["number_country_pairs"].astype(float)).replace(
                [np.inf, -np.inf], np.nan
            )

    # CONFLICT MORTALITY ###########
    if columns_conflict_mortality:
        # Add normalised indicators
        for column_name in columns_conflict_mortality:
            # Add per country indicator
            column_name_new = f"{column_name}_per_capita"
            tb[column_name_new] = (
                (100000 * tb[column_name].astype(float) / tb["population"])
                .replace([np.inf, -np.inf], np.nan)
                .astype(float)
            )

    # Drop intermediate columns
    tb = tb.drop(columns=["number_countries", "number_country_pairs", "population"])

    return tb


class Normaliser:
    """Normalise indicators."""

    country_column: str

    def code_to_region(self: Self) -> None:
        """Convert code to region name."""
        raise NotImplementedError("Subclasses must implement this method")

    @classmethod
    def add_num_countries_per_year(cls: Type[Self], tb: Table) -> Table:
        """Get number of countries (and country-pairs) per region per year and add it to the table.

        `tb` is expected to be the table cow_ssm_system from the cow_ssm dataset.
        """
        # Get number of country-pairs per region per year
        tb["num_country_pairs"] = (tb["num_countries"] * (tb["num_countries"] - 1) / 2).astype(int)

        return tb

    @classmethod
    def add_indicators(cls: Type[Self], tb: Table, tb_codes: Table, columns_to_scale: List[str]) -> Table:
        """Scale columns `columns_to_scale` based on the number of countries (and country-pairs) in each region and year.

        For each indicator listed in `columns_to_scale`, two new columns are added to the table:
        - `{indicator}_per_country`: the indicator value divided by the number of countries in the region and year.
        - `{indicator}_per_country_pair`: the indicator value divided by the number of country-pairs in the region and year.
        """
        # From raw cow_ssm_system table get number of countryes (and country-pairs) per region per year
        tb_codes = cls.add_num_countries_per_year(tb_codes)
        # Merge with main table
        tb = tb.merge(tb_codes, on=["year", "region"], how="left")

        # Add normalised indicators
        for col in columns_to_scale:
            tb[f"{col}_per_country"] = tb[col] / tb["num_countries"]
            tb[f"{col}_per_country_pair"] = tb[col] / tb["num_country_pairs"]

        # Drop intermediate columns
        tb = tb.drop(columns=["num_countries", "num_country_pairs"])

        return tb


class COWNormaliser(Normaliser):
    """Normalise COW data based on the number of countries (and country-pairs) in each region and year."""

    country_column: str = "statenme"

    @classmethod
    def code_to_region(cls: Type[Self], cow_code: int) -> str:
        """Convert code to region name."""
        match cow_code:
            case c if 2 <= c <= 165:
                return "Americas"
            case c if 200 <= c <= 399:
                return "Europe"
            case c if 402 <= c <= 626:
                return "Africa"
            case c if 630 <= c <= 698:
                return "Middle East"
            case c if 700 <= c <= 999:
                return "Asia and Oceania"
            case _:
                raise ValueError(f"Invalid COW code: {cow_code}")
