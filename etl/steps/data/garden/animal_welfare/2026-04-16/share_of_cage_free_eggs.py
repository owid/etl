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


def add_share_of_eggs_in_us(tb_us: Table) -> Table:
    tb_us = tb_us.copy()

    # Compute share of cage-free hens from flock sizes.
    # NOTE: Unlike the UK (where we have egg counts), for the US we only have flock sizes,
    # so we use the share of cage-free hens as a proxy for the share of cage-free eggs.
    total_hens = tb_us["caged"] + tb_us["cage_free"]
    tb_us["share_of_eggs_cage_free"] = 100 * tb_us["cage_free"] / total_hens
    tb_us["share_of_eggs_in_cages"] = 100 - tb_us["share_of_eggs_cage_free"]

    # Update their units.
    tb_us["share_of_eggs_cage_free"].metadata.unit = "%"
    tb_us["share_of_eggs_cage_free"].metadata.short_unit = "%"
    tb_us["share_of_eggs_in_cages"].metadata.unit = "%"
    tb_us["share_of_eggs_in_cages"].metadata.short_unit = "%"

    return tb_us


def run() -> None:
    #
    # Load inputs.
    #
    # Load US egg production dataset.
    ds_us = paths.load_dataset("us_egg_production")
    tb_us = ds_us.read("us_egg_production")

    # Load UK egg statistics dataset.
    ds_uk = paths.load_dataset("uk_egg_statistics")
    tb_uk = ds_uk.read("uk_egg_statistics")

    #
    # Process data.
    #
    # Add the share of UK eggs in cages and cage-free.
    tb_uk = add_share_of_eggs_in_uk(tb_uk=tb_uk)

    # Add the share of US eggs in cages and cage-free.
    tb_us = add_share_of_eggs_in_us(tb_us=tb_us)

    # Combine data from different countries.
    columns = ["country", "year", "share_of_eggs_in_cages", "share_of_eggs_cage_free"]
    tb = pr.concat([tb_uk[columns], tb_us[columns]], ignore_index=True, short_name=paths.short_name)

    # Improve table format.
    tb = tb.format()

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb])

    # Save new garden dataset.
    ds_garden.save()
