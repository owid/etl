"""Load a meadow dataset and create a garden dataset."""

from datetime import datetime as dt
from typing import List, Optional

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Only for table tb_regions:
# The current list of members goes until 2016, we artificially extend it until 2023, preserving the last value
EXPECTED_LAST_YEAR = 2017
LAST_YEAR = 2022  # Update to extend it further in time


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("gleditsch")

    # Read table from meadow dataset.
    tb = ds_meadow["gleditsch"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Create new table
    tb_regions = create_table_countries_in_region(tb)
    tables = [
        tb.set_index(["id", "start", "end"], verify_integrity=True).sort_index(),
        tb_regions.set_index(["region", "year"], verify_integrity=True).sort_index(),
    ]

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=tables, check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def create_table_countries_in_region(tb: Table):
    """Create table with number of countries per region per year."""
    # Create new table
    tb_regions = tb.copy()

    # Get start and end years
    tb_regions["start"] = tb_regions["start"].apply(_get_start_year)
    tb_regions["end"] = tb_regions["end"].apply(_get_end_year)

    # Expand observations: go from (start, end) to (year_obs)
    tb_regions = expand_observations(tb_regions, "start", "end")

    # Sanity check
    ## Check that there is only one country per (id, year)
    ## Note that there are multiple countries per ID (max 2; in this case there is one with ID 580: 'Madagascar' and 'Madagascar (Malagasy)')
    assert (
        tb_regions.groupby(["id", "year"])["country"].nunique().max() == 1
    ), "Multiple `country` values for same `id` and `year`"

    # Keep relevant columns
    tb_regions = tb_regions[["id", "year"]]

    # Add region names
    tb_regions["region"] = tb_regions["id"].apply(code_to_region)

    # Get number of countries per region per year
    tb_regions = (
        tb_regions.groupby(["region", "year"], as_index=False)
        .agg({"id": "nunique"})
        .rename(columns={"id": "number_countries"})
    )

    # Get numbers for World
    tb_regions_world = tb_regions.groupby(["year"], as_index=False).agg({"number_countries": "sum"})
    tb_regions_world["region"] = "World"

    # Combine
    tb_regions = pr.concat([tb_regions, tb_regions_world], ignore_index=True)

    # Add short name
    tb_regions.metadata.short_name = "gleditsch_regions"

    # Check latest year is as expected, drop year column
    tb_last = tb_regions.sort_values("year").drop_duplicates(subset=["region"], keep="last")
    assert (tb_last.year.unique() == EXPECTED_LAST_YEAR).all(), "Last year is not 2016!"
    tb_last = tb_last.drop(columns=["year"])

    # Cross merge with missing years
    tb_all_years = Table(pd.RangeIndex(EXPECTED_LAST_YEAR + 1, LAST_YEAR + 1), columns=["year"])
    tb_last = tb_last[["region", "number_countries"]].merge(tb_all_years, how="cross")

    # Add to main table
    tb_regions = pr.concat([tb_regions, tb_last], ignore_index=True).sort_values(["region", "year"])

    return tb_regions


def expand_observations(
    tb: Table, col_year_start: str, col_year_end: str, cols_scale: Optional[List[str]] = None, rounding: bool = True
) -> Table:
    """Expand to have a row per (year, conflict).

    See function in /home/lucas/repos/etl/etl/steps/data/garden/war/2023-09-21/shared.py for complete docstring info.

    Difference in this one is that upper inequality is strict!
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
    tb = tb[(tb["year"] >= tb[col_year_start]) & (tb["year"] < tb[col_year_end])]

    return tb


def code_to_region(cow_code: int) -> str:
    """Convert code to region name."""
    match cow_code:
        case c if 2 <= c <= 199:
            return "Americas"
        case c if 200 <= c <= 399:
            return "Europe"
        case c if 400 <= c <= 626:
            return "Africa"
        case c if 630 <= c <= 699:
            return "Middle East"
        case c if 700 <= c <= 999:
            return "Asia and Oceania"
        case _:
            raise ValueError(f"Invalid COW code: {cow_code}")


def _get_start_year(date_str):
    date = dt.strptime(date_str, "%d:%m:%Y")
    return date.year


def _get_end_year(date_str):
    date = dt.strptime(date_str, "%d:%m:%Y")
    if (date.month == 12) & (date.day == 31):
        return date.year + 1
    return date.year
