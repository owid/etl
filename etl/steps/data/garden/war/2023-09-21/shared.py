from typing import List, Optional, Type

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
        tb["region"] = tb["ccode"].apply(cls.code_to_region)

        # Get number of countries per region per year
        tb = (
            tb.groupby(["region", "year"], as_index=False)
            .agg({cls.country_column: "nunique"})
            .rename(columns={cls.country_column: "num_countries"})
        )
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
