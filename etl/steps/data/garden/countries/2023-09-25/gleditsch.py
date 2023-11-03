"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Dataset, Table
from shared import (
    add_latest_years_with_constant_num_countries,
    add_population_to_table,
    init_table_countries_in_region,
)

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Only for table tb_regions:
# The current list of members goes until 2017, we artificially extend it until year of latest 31st of December
EXPECTED_LAST_YEAR = 2017


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

    # Minor fix
    tb.loc[tb["country"] == "German Federal Republic", "end"] = "02:10:1990"

    # Format table
    tb_formatted = format_table(tb)

    # Create new table
    tb_regions = create_table_countries_in_region(tb_formatted, ds_pop)

    # Population table
    tb_pop = add_population_to_table(tb_formatted, ds_pop)

    # Combine tables
    tb_regions = tb_regions.merge(tb_pop, how="left", on=["region", "year"])

    # Add to table list
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
    tb_regions = add_latest_years_with_constant_num_countries(
        tb_regions,
        column_year="year",
        expected_last_year=EXPECTED_LAST_YEAR,
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
