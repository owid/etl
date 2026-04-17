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


def add_share_of_eggs_in_us(tb_usda: Table, tb_thl: Table) -> Table:
    tb_usda = tb_usda.copy()

    # Compute share of cage-free hens from USDA flock sizes (2012+).
    # NOTE: Unlike the UK (where we have egg counts), for the US we only have flock sizes,
    # so we use the share of cage-free hens as a proxy for the share of cage-free eggs.
    total_hens = tb_usda["caged"] + tb_usda["cage_free"]
    tb_usda["share_of_eggs_cage_free"] = 100 * tb_usda["cage_free"] / total_hens
    tb_usda["share_of_eggs_in_cages"] = 100 - tb_usda["share_of_eggs_cage_free"]

    # Update their units.
    tb_usda["share_of_eggs_cage_free"].metadata.unit = "%"
    tb_usda["share_of_eggs_cage_free"].metadata.short_unit = "%"
    tb_usda["share_of_eggs_in_cages"].metadata.unit = "%"
    tb_usda["share_of_eggs_in_cages"].metadata.short_unit = "%"

    columns = ["country", "year", "share_of_eggs_in_cages", "share_of_eggs_cage_free"]
    tb_usda = tb_usda[columns]

    # For 2007-2011, use share of cage-free hens from THL (originally sourced from USDA reports).
    tb_thl = tb_thl[tb_thl["year"] < 2012][["country", "year", "share_of_hens_cage_free"]].rename(
        columns={"share_of_hens_cage_free": "share_of_eggs_cage_free"}
    )
    tb_thl["share_of_eggs_in_cages"] = 100 - tb_thl["share_of_eggs_cage_free"]

    return pr.concat([tb_thl[columns], tb_usda], ignore_index=True)


def run() -> None:
    #
    # Load inputs.
    #
    # Load new USDA egg production dataset (2012+).
    ds_usda = paths.load_dataset("us_egg_production", version="2026-04-16")
    tb_usda = ds_usda.read("us_egg_production")

    # Load old THL egg production dataset for pre-2012 share data.
    ds_thl = paths.load_dataset("us_egg_production", version="2023-08-03")
    tb_thl = ds_thl.read("us_egg_production_share_cage_free")

    # Load UK egg statistics dataset.
    ds_uk = paths.load_dataset("uk_egg_statistics")
    tb_uk = ds_uk.read("uk_egg_statistics")

    #
    # Process data.
    #
    # Add the share of UK eggs in cages and cage-free.
    tb_uk = add_share_of_eggs_in_uk(tb_uk=tb_uk)

    # Add the share of US eggs in cages and cage-free.
    tb_usda = add_share_of_eggs_in_us(tb_usda=tb_usda, tb_thl=tb_thl)

    # Combine data from different countries.
    columns = ["country", "year", "share_of_eggs_in_cages", "share_of_eggs_cage_free"]
    tb = pr.concat([tb_uk[columns], tb_usda[columns]], ignore_index=True, short_name=paths.short_name)

    # Improve table format.
    tb = tb.format()

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb])

    # Save new garden dataset.
    ds_garden.save()
