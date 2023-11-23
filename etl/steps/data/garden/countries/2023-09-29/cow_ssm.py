"""Load a meadow dataset and create a garden dataset."""

from typing import Optional, cast

import numpy as np
import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Dataset, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Only for table tb_regions:
# The current list of members goes until 2016, we artificially extend it until 2022, preserving the last value
EXPECTED_LAST_YEAR = 2016
LAST_YEAR = 2022  # Update to extend it further in time (until year with last 31 December)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("cow_ssm")
    # Load population table
    ds_pop = paths.load_dataset("population")

    # Read table from meadow dataset.
    tb_system = ds_meadow["cow_ssm_system"].reset_index()
    tb_states = ds_meadow["cow_ssm_states"].reset_index()
    tb_majors = ds_meadow["cow_ssm_majors"].reset_index()

    #
    # Process data.
    #
    # Checks
    assert tb_system.groupby("ccode")["stateabb"].nunique().max() == 1, "Multiple `stateabb` values for same `ccode`"
    assert tb_states.groupby("ccode")["statenme"].nunique().max() == 1, "Multiple `statenme` values for same `ccode`"
    assert tb_majors.groupby("ccode")["stateabb"].nunique().max() == 1, "Multiple `stateabb` values for same `ccode`"

    # Add COW state names to those missing them
    tb_codes = tb_states[["stateabb", "statenme"]].drop_duplicates()
    ## tb_system
    length_first = tb_system.shape[0]
    tb_system = tb_system.merge(tb_codes, on="stateabb")
    assert tb_system.shape[0] == length_first, "Some `state_name` values are missing after merge (tb_system)!"
    ## tb_majors
    length_first = tb_majors.shape[0]
    tb_majors = tb_majors.merge(tb_codes, on="stateabb")
    assert tb_majors.shape[0] == length_first, "Some `state_name` values are missing after merge (tb_majors)!"

    # Harmonize country names
    tb_system = geo.harmonize_countries(df=tb_system, countries_file=paths.country_mapping_path, country_col="statenme")
    tb_states = geo.harmonize_countries(df=tb_states, countries_file=paths.country_mapping_path, country_col="statenme")
    tb_majors = geo.harmonize_countries(df=tb_majors, countries_file=paths.country_mapping_path, country_col="statenme")

    # Minor format
    tb = tb_system.copy()
    tb["region"] = tb_system["ccode"].apply(code_to_region)

    # Create new table
    tb_regions = create_table_countries_in_region(tb)

    # Add missing years
    ## Create grid of possible values
    assert tb_regions["number_countries"].notna().all(), "No missing years to add!"
    values_regions = set(tb_regions["region"])
    values_year = np.arange(tb_regions["year"].min(), tb_regions["year"].max() + 1)
    values_possible = [
        values_regions,
        values_year,
    ]
    # ## Set new index
    columns = ["region", "year"]
    new_idx = pd.MultiIndex.from_product(values_possible, names=columns)
    tb_regions = tb_regions.set_index(columns).reindex(new_idx).reset_index()
    # ## Replace NaNs with zeroes & sort
    tb_regions["number_countries"] = tb_regions["number_countries"].fillna(0)
    tb_regions = tb_regions.sort_values(["region", "year"])

    # Population table
    tb_pop = add_population_to_table(tb, ds_pop)

    # Combine tables
    tb_regions = tb_regions.merge(tb_pop, how="left", on=["region", "year"])

    # Get table with id, year, country (whenever that country was present)
    tb_countries = create_table_country_years(tb)

    # Group tables and format tables
    tables = [
        tb_system.set_index(["ccode", "year"], verify_integrity=True).sort_index(),
        tb_states.set_index(["ccode", "styear", "stmonth", "stday", "endyear", "endmonth", "endday"]).sort_index(),
        tb_majors.set_index(["ccode", "styear", "stmonth", "stday", "endyear", "endmonth", "endday"]).sort_index(),
        tb_regions.set_index(["region", "year"], verify_integrity=True).sort_index(),
        tb_countries.set_index(["id", "year"], verify_integrity=True).sort_index(),
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


def create_table_countries_in_region(tb_system: Table):
    """Create table with number of countries per region per year."""
    # Create new table
    tb_regions = tb_system.copy()

    # Get number of countries per region per year
    tb_regions = (
        tb_regions.groupby(["region", "year"], as_index=False)
        .agg({"statenme": "nunique"})
        .rename(columns={"statenme": "number_countries"})
    )

    # Get numbers for World
    tb_regions_world = tb_regions.groupby(["year"], as_index=False).agg({"number_countries": "sum"})
    tb_regions_world["region"] = "World"

    # Combine
    tb_regions = pr.concat([tb_regions, tb_regions_world], ignore_index=True, short_name="cow_ssm_regions")

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


def code_to_region(cow_code: int) -> str:
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


def add_population_to_table(tb: Table, ds_pop: Dataset, country_col: str = "country") -> Table:
    """Add population.

    1. Get list of countries from latest available year. That is, we only have one row per country.
    2. Duplicate these entries for each year from first available to latest available year. As if they existed.
        This is because the population dataset tracks population back in time with current countries' borders.
    3. Merge with population dataset

    NOTE: Duplicated from etl/steps/data/garden/countries/2023-09-25/shared.py
    """
    YEAR_MAX = tb["year"].max()
    YEAR_MIN = tb["year"].min()
    # Get last year data
    tb_last = tb[tb["year"] == YEAR_MAX].drop(columns=["year"])

    # Extend to all years
    tb_all_years = Table(pd.RangeIndex(YEAR_MIN, LAST_YEAR + 1), columns=["year"])
    tb_pop = tb_last.merge(tb_all_years, how="cross")

    # Add population
    tb_pop = geo.add_population_to_table(tb_pop, ds_pop, country_col="statenme")

    # Estimate population by region
    tb_pop_regions = tb_pop.groupby(["year", "region"], as_index=False)[["population"]].sum()

    # Estimate world population
    tb_pop_world = tb_pop.groupby(["year"], as_index=False)[["population"]].sum()
    tb_pop_world["region"] = "World"

    # Combine
    tb_pop = pr.concat([tb_pop_regions, tb_pop_world], ignore_index=True)

    return tb_pop


def create_table_country_years(tb: Table) -> Table:
    """Create table with each country present in a year."""
    tb_countries = tb[["ccode", "year", "statenme"]].copy()

    tb_countries = tb_countries.rename(columns={"ccode": "id", "statenme": "country"})

    # define mask for last year
    mask = tb_countries["year"] == EXPECTED_LAST_YEAR

    tb_last = fill_timeseries(
        tb_countries[mask].drop(columns="year"),
        EXPECTED_LAST_YEAR + 1,
        LAST_YEAR,
    )

    tb = pr.concat([tb_countries, tb_last], ignore_index=True, short_name="cow_ssm_countries")

    tb["year"] = tb["year"].astype(int)

    ## Serbia and Montenegro, Serbia
    tb["country"] = tb["country"].astype(str)
    # tb.loc[(tb["id"] == 345) & (tb["year"] >= 1992) & (tb["year"] < 2006), "country"] = "Serbia and Montenegro"
    tb.loc[(tb["id"] == 345) & (tb["year"] >= 2006), "country"] = "Serbia"

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
