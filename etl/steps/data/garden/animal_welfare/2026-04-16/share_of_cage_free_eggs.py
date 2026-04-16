"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def add_share_of_eggs_in_uk(tb_uk: Table) -> Table:
    tb_uk = tb_uk.copy()

    # Add the share of UK eggs in cages and cage-free.
    tb_uk["share_of_eggs_in_cages"] = 100 * tb_uk["number_of_eggs_from_enriched_cages"] / tb_uk["number_of_eggs_all"]
    tb_uk["share_of_eggs_cage_free"] = 100 - tb_uk["share_of_eggs_in_cages"]

    # Update their units.
    tb_uk["share_of_eggs_cage_free"].metadata.unit = "%"
    tb_uk["share_of_eggs_cage_free"].metadata.short_unit = "%"
    tb_uk["share_of_eggs_in_cages"].metadata.unit = "%"
    tb_uk["share_of_eggs_in_cages"].metadata.short_unit = "%"

    return tb_uk


def run_sanity_checks(tb_us: Table) -> None:
    # For the US, we have data from 2007 only on the share of cage-free hens, not eggs (for eggs, data starts in 2016).
    # Check that those percentages (where there is data for both) are similar
    error = "Expected less than 8 percent difference between the share of cage-free hens and eggs."
    assert (
        100
        * abs(tb_us["share_of_hens_cage_free"] - tb_us["share_of_eggs_cage_free"])
        / tb_us["share_of_eggs_cage_free"]
    ).max() < 8, error


def run() -> None:
    #
    # Load inputs.
    #
    # Load US egg production dataset and read its table on the share of cage-free hens and eggs.
    ds_us = paths.load_dataset("us_egg_production")
    tb_us = ds_us.read("us_egg_production_share_cage_free")

    # Load UK egg statistics dataset and read its main table.
    ds_uk = paths.load_dataset("uk_egg_statistics")
    tb_uk = ds_uk.read("uk_egg_statistics")

    #
    # Process data.
    #
    # Add the share of UK eggs in cages and cage-free.
    tb_uk = add_share_of_eggs_in_uk(tb_uk=tb_uk)

    # Run sanity checks.
    run_sanity_checks(tb_us=tb_us)

    # Assume that the share of cage-free eggs is the same as the share of cage-free hens.
    tb_us = tb_us.drop(columns=["share_of_eggs_cage_free"]).rename(
        columns={"share_of_hens_cage_free": "share_of_eggs_cage_free"}, errors="raise"
    )

    # Add the share of US eggs in cages.
    tb_us["share_of_eggs_in_cages"] = 100 - tb_us["share_of_eggs_cage_free"]

    # Combine data from different countries.
    columns = ["country", "year", "share_of_eggs_in_cages", "share_of_eggs_cage_free"]
    tb = pr.concat([tb_uk[columns], tb_us[columns]], ignore_index=True, short_name=paths.short_name)

    # Improve table format.
    tb = tb.format(keys=["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb])

    # Save new garden dataset.
    ds_garden.save()
