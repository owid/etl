from datetime import datetime as dt
from typing import Optional, cast

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


def add_latest_years_with_constant_num_countries(
    tb: Table,
    column_year: str,
    expected_last_year: int,
) -> Table:
    """Extend data until LAST_YEAR with constant number of countries.

    Data stops at expected_last_year, extend it until LAST_YEAR with constant number of countries.
    """
    # Check latest year is as expected, drop year column
    tb_last = tb.sort_values(column_year).drop_duplicates(subset=["region"], keep="last")
    assert (tb_last.year.unique() == expected_last_year).all(), f"Last year is not {expected_last_year}!"
    tb_last = tb_last.drop(columns=[column_year])

    # Cross merge with missing years
    tb_all_years = Table(pd.RangeIndex(expected_last_year + 1, stop=LAST_YEAR + 1), columns=[column_year])
    tb_last = tb_last[["region", "number_countries"]].merge(tb_all_years, how="cross")

    # Add to main table
    tb = pr.concat([tb, tb_last], ignore_index=True).sort_values(["region", column_year])

    return tb


def expand_observations(tb: Table, col_year_start: str, col_year_end: str) -> Table:
    """Expand to have a row per (year, conflict).

    See function in /home/lucas/repos/etl/etl/steps/data/garden/war/2023-09-21/shared.py for complete docstring info.

    Difference in this one is that upper inequality is strict!
    """
    # Add missing years for each triplet ("warcode", "campcode", "ccode")
    YEAR_MIN = tb[col_year_start].min()
    YEAR_MAX = tb[col_year_end].max()
    if "year" in tb.columns:
        raise ValueError("Column 'year' already in table!")
    else:
        tb = fill_timeseries(tb, YEAR_MIN, YEAR_MAX)
    # Filter only entries that actually existed
    tb = tb[(tb["year"] >= tb[col_year_start]) & (tb["year"] < tb[col_year_end])]

    return tb


def fill_timeseries(
    tb: Table,
    year_min: Optional[int],
    year_max: Optional[int],
    default_min: bool = False,
    default_max: bool = False,
    col_year_start: Optional[str] = None,
    col_year_end: Optional[str] = None,
    filter_times: bool = False,
) -> Table:
    """Complement table with missing years."""
    # Get starting year
    if default_min:
        if col_year_start in tb.columns:
            year_min = tb[col_year_start].min()
        else:
            raise ValueError(f"{col_year_start} not in table columns!")
    elif year_min is None:
        raise ValueError("Either `year_min` must be a value or `default_min` must be True")
    # Get ending year
    if default_max:
        if (col_year_end) and (col_year_end in tb.columns):
            year_max = tb[col_year_end].max()
        else:
            raise ValueError(f"{col_year_end} not in table columns!")
    elif year_max is None:
        raise ValueError("Either `year_max` must be a value or `default_max` must be True")

    # Cross merge with missing years
    tb_all_years = Table(pd.RangeIndex(year_min, cast(int, year_max) + 1), columns=["year"])
    if "year" in tb.columns:
        raise ValueError("Column 'year' already in table! Please drop it from `tb`.")
    tb = tb.merge(tb_all_years, how="cross")

    # Only keep years that 'make sense'
    if filter_times:
        if (col_year_end and (col_year_end not in tb.columns)) or (
            col_year_start and (col_year_start not in tb.columns)
        ):
            raise ValueError(f"Columns {col_year_start} and {col_year_end} must be in table columns!")
        else:
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
