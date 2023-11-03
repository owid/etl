"""Load a meadow dataset and create a garden dataset."""

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

    # Population table
    tb_pop = add_population_to_table(tb, ds_pop)

    # Combine tables
    tb_regions = tb_regions.merge(tb_pop, how="left", on=["region", "year"])

    # Group tables and format tables
    tables = [
        tb_system.set_index(["ccode", "year"], verify_integrity=True).sort_index(),
        tb_states.set_index(["ccode", "styear", "stmonth", "stday", "endyear", "endmonth", "endday"]).sort_index(),
        tb_majors.set_index(["ccode", "styear", "stmonth", "stday", "endyear", "endmonth", "endday"]).sort_index(),
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
