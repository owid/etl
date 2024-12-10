"""Load a meadow dataset and create a garden dataset."""

from datetime import datetime as dt

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Dataset, Table

from etl.data_helpers import geo
from etl.data_helpers.misc import expand_time_column, explode_rows_by_time_range
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Only for table tb_regions:
# The current list of members goes until 2017, we artificially extend it until year of latest 31st of December
EXPECTED_LAST_YEAR = 2017
# Only for table tb_regions:
# Latest year to have had a 31st of December
LAST_YEAR = 2023  # Update to extend it further in time


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load gleditsch table
    ds_meadow = paths.load_dataset("gleditsch")
    tb = ds_meadow["gleditsch"].reset_index()
    # Load population table
    ds_pop = paths.load_dataset("population")

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Format table
    tb_formatted = format_table(tb)

    # Minor fix
    ## GW code 260 should be referred to as 'West Germany' until 1990, then as 'Germany'
    tb_formatted.loc[(tb_formatted["id"] == 260) & (tb_formatted["year"] >= 1990), "country"] = "Germany"

    # Create new table
    tb_regions = create_table_countries_in_region(tb_formatted, ds_pop)

    # Population table
    tb_pop = add_population_to_table(tb_formatted, ds_pop)

    # Combine tables
    tb_regions = tb_regions.merge(tb_pop, how="left", on=["region", "year"])

    # Get table with id, year, country (whenever that country was present)
    tb_countries = create_table_country_years(tb_formatted)

    # Add to table list
    tables = [
        tb.format(["id", "start", "end"]),
        tb_countries.format(["id", "year"]),
        tb_regions.format(["region", "year"]),
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


def format_table(tb: Table) -> Table:
    """Format table.

    - Create years
    - Expand observations
    - Map countries to regions
    """
    tb = init_table_countries_in_region(
        tb,
        date_format="%d:%m:%Y",
        column_start="start",
        column_end="end",
        column_id="id",
    )

    # Get region name
    tb["region"] = tb["id"].apply(code_to_region)

    return tb


def create_table_country_years(tb: Table) -> Table:
    """Create table with each country present in a year."""
    tb_countries = tb[["id", "year", "country"]].copy()

    # define mask for last year
    mask = tb_countries["year"] == EXPECTED_LAST_YEAR

    tb_all_years = Table(pd.RangeIndex(EXPECTED_LAST_YEAR + 1, LAST_YEAR + 1), columns=["year"])
    tb_last = tb_countries[mask].drop(columns="year").merge(tb_all_years, how="cross")

    tb = pr.concat([tb_countries, tb_last], ignore_index=True, short_name="gleditsch_countries")

    return tb


def create_table_countries_in_region(tb: Table, ds_pop: Dataset) -> Table:
    """Create table with number of countries in each region per year."""
    # Get number of countries per region per year
    tb_regions = (
        tb.groupby(["region", "year"], as_index=False).agg({"id": "nunique"}).rename(columns={"id": "number_countries"})
    )

    # Get numbers for World
    tb_world = tb.groupby(["year"], as_index=False).agg({"id": "nunique"}).rename(columns={"id": "number_countries"})
    tb_world["region"] = "World"

    # Combine
    tb_regions = pr.concat([tb_regions, tb_world], ignore_index=True, short_name="gleditsch_regions")

    # Finish by adding missing last years
    tb_regions = expand_time_column(
        df=tb_regions,
        dimension_col=["region"],
        time_col="year",
        method="none",
        until_time=LAST_YEAR,
        fillna_method="ffill",
    )
    return tb_regions


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
            raise ValueError(f"Invalid GW code: {cow_code}")


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

    def _get_start_year(date_str: str, date_format: str) -> int:
        date = dt.strptime(date_str, date_format)
        return date.year

    def _get_end_year(date_str: str, date_format: str) -> int:
        date = dt.strptime(date_str, date_format)
        if (date.month == 12) & (date.day == 31):
            return date.year + 1
        return date.year

    # Create new table
    tb_regions = tb.copy()

    # Get start and end years
    tb_regions[column_start] = tb_regions[column_start].apply(_get_start_year, date_format=date_format)
    tb_regions[column_end] = tb_regions[column_end].apply(_get_end_year, date_format=date_format)

    # Expand observations: go from (start, end) to (year_obs)
    tb_regions = explode_rows_by_time_range(
        tb_regions,
        "start",
        "end",
        "year",
    )

    # Sanity check
    ## Check that there is only one country per (id, year)
    ## Note that there are multiple countries per ID (max 2; in this case there is one with ID 580: 'Madagascar' and 'Madagascar (Malagasy)')
    assert (
        tb_regions.groupby([column_id, column_year])[column_country].nunique().max() == 1
    ), f"Multiple `country` values for same `{column_id}` and `year`"

    # Keep relevant columns
    tb_regions = tb_regions[[column_id, column_year, column_country]]

    return tb_regions


def add_population_to_table(
    tb: Table, ds_pop: Dataset, country_col: str = "country", region_alt: bool = False
) -> Table:
    """Add population to table.

    1. Get list of countries from latest available year. That is, we only have one row per country.
    2. Duplicate these entries for each year from first available to latest available year. As if they existed.
        This is because the population dataset tracks population back in time with current countries' borders.
    3. Merge with population dataset
    """
    YEAR_MAX = tb["year"].max()
    YEAR_MIN = tb["year"].min()
    # Get last year data
    tb_last = tb[tb["year"] == YEAR_MAX].drop(columns=["year"])

    # Extend to all years
    tb_all_years = Table(pd.RangeIndex(YEAR_MIN, LAST_YEAR + 1), columns=["year"])
    tb_pop = tb_last.merge(tb_all_years, how="cross")

    # Add population
    tb_pop = geo.add_population_to_table(tb_pop, ds_pop, country_col=country_col)

    # Estimate population by region
    tb_pop_regions = tb_pop.groupby(["year", "region"], as_index=False)[["population"]].sum()

    # Estimate world population
    tb_pop_world = tb_pop.groupby(["year"], as_index=False)[["population"]].sum()
    tb_pop_world["region"] = "World"

    if region_alt:
        # Estimate population by region
        tb_pop_regions_alt = tb_pop.groupby(["year", "region_alt"], as_index=False)[["population"]].sum()
        tb_pop_regions_alt = tb_pop_regions_alt.rename(columns={"region_alt": "region"})
        tb_pop_regions_alt = tb_pop_regions_alt[tb_pop_regions_alt["region"] != "Rest"]
        # Combine
        tb_pop = pr.concat([tb_pop_regions, tb_pop_regions_alt, tb_pop_world], ignore_index=True)
    else:
        tb_pop = pr.concat([tb_pop_regions, tb_pop_world], ignore_index=True)
    return tb_pop
