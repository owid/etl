"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

from .shared import (
    add_latest_years_with_constat_num_countries,
    init_table_countries_in_region,
)

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Only for table tb_regions:
# The current list of members goes until 2017, we artificially extend it until year of latest 31st of December
EXPECTED_LAST_YEAR = 2017


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


def create_table_countries_in_region(tb: Table) -> Table:
    """Create table with number of countries in each region per year."""
    tb = init_table_countries_in_region(
        tb,
        date_format="%d:%m:%Y",
        column_start="start",
        column_end="end",
        column_id="id",
    )

    # Get region name
    tb["region"] = tb["id"].apply(code_to_region)
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
    tb_regions = add_latest_years_with_constat_num_countries(
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
