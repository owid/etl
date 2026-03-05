"""Detect countries that have decoupled per capita GDP growth from per capita consumption-based CO2 emissions."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load Global Carbon Budget dataset and read its main table.
    ds_garden = paths.load_dataset("gdp_and_co2_decoupling")
    tb_decoupled = ds_garden.read("gdp_and_co2_decoupling")

    #
    # Process data.
    #
    # Prepare a table with only the first (peak emissions year) and last year for each country.
    tb_first_and_last_year = pr.concat(
        [
            tb_decoupled.groupby("country", as_index=False).first(),
            tb_decoupled.groupby("country", as_index=False).last(),
        ],
        ignore_index=True,
    )
    # Improve table format.
    tb_first_and_last_year = tb_first_and_last_year.format(
        ["country", "year"], short_name=paths.short_name + "_first_and_last_year"
    )

    #
    # Save outputs.
    #
    ds = paths.create_dataset(tables=[tb_first_and_last_year], formats=["csv"])
    ds.save()
