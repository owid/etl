from datetime import datetime as dt
from typing import Literal

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table

# Only for table tb_regions:
# Latest year to have had a 31st of December
LAST_YEAR = 2022  # Update to extend it further in time
# ISD Special regions


def init_table_countries_in_region(
    tb: Table,
    date_format: str,
    column_start: str,
    column_end: str,
    column_id: str,
    column_country: str = "country",
    column_year: str = "year",
) -> Table:
    """Create table with number of countries per region per year."""
    # Create new table
    tb_regions = tb.copy()

    # Get start and end years
    tb_regions[column_start] = tb_regions[column_start].apply(_get_start_year, date_format=date_format)
    tb_regions[column_end] = tb_regions[column_end].apply(_get_end_year, date_format=date_format)

    # Expand observations: go from (start, end) to (year_obs)
    tb_regions = expand_observations(tb_regions, "start", "end")

    # Sanity check
    ## Check that there is only one country per (id, year)
    ## Note that there are multiple countries per ID (max 2; in this case there is one with ID 580: 'Madagascar' and 'Madagascar (Malagasy)')
    assert (
        tb_regions.groupby([column_id, column_year])[column_country].nunique().max() == 1
    ), f"Multiple `country` values for same `{column_id}` and `year`"

    # Keep relevant columns
    tb_regions = tb_regions[[column_id, column_year, column_country]]

    return tb_regions


def add_latest_years_with_constat_num_countries(tb_regions: Table, column_year: str, expected_last_year: int) -> Table:
    """Extend data until LAST_YEAR with constant number of countries.

    Data stops at expected_last_year, extend it until LAST_YEAR with constant number of countries.
    """
    # Check latest year is as expected, drop year column
    tb_last = tb_regions.sort_values(column_year).drop_duplicates(subset=["region"], keep="last")
    assert (tb_last.year.unique() == expected_last_year).all(), f"Last year is not {expected_last_year}!"
    tb_last = tb_last.drop(columns=[column_year])

    # Cross merge with missing years
    tb_all_years = Table(pd.RangeIndex(expected_last_year + 1, stop=LAST_YEAR + 1), columns=[column_year])
    tb_last = tb_last[["region", "number_countries"]].merge(tb_all_years, how="cross")

    # Add to main table
    tb_regions = pr.concat([tb_regions, tb_last], ignore_index=True).sort_values(["region", column_year])

    return tb_regions


def expand_observations(tb: Table, col_year_start: str, col_year_end: str) -> Table:
    """Expand to have a row per (year, conflict).

    See function in /home/lucas/repos/etl/etl/steps/data/garden/war/2023-09-21/shared.py for complete docstring info.

    Difference in this one is that upper inequality is strict!
    """
    # Add missing years for each triplet ("warcode", "campcode", "ccode")
    YEAR_MIN = tb[col_year_start].min()
    YEAR_MAX = tb[col_year_end].max()
    tb_all_years = Table(pd.RangeIndex(YEAR_MIN, YEAR_MAX + 1), columns=["year"])
    tb = tb.merge(tb_all_years, how="cross")
    # Filter only entries that actually existed
    tb = tb[(tb["year"] >= tb[col_year_start]) & (tb["year"] < tb[col_year_end])]

    return tb


def _get_start_year(date_str: str, date_format: str) -> int:
    date = dt.strptime(date_str, date_format)
    return date.year


def _get_end_year(date_str: str, date_format: str) -> int:
    date = dt.strptime(date_str, date_format)
    if (date.month == 12) & (date.day == 31):
        return date.year + 1
    return date.year
