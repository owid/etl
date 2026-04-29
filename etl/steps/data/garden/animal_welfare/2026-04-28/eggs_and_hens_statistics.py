"""Combine hen housing-system data from multiple sources to give global coverage."""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder

paths = PathFinder(__file__)

SHARE_OF_HENS_COLUMNS = [
    "share_of_hens_in_cages",
    "share_of_hens_cage_free",
    "share_of_hens_in_barns",
    "share_of_hens_free_range_not_organic",
    "share_of_hens_free_range_organic",
]

NUMBER_OF_HENS_COLUMNS = [
    "number_of_hens_all",
    "number_of_hens_in_cages",
    "number_of_hens_cage_free",
    "number_of_hens_in_barns",
    "number_of_hens_free_range_not_organic",
    "number_of_hens_free_range_organic",
]

HEN_COLUMNS = ["country", "year"] + SHARE_OF_HENS_COLUMNS + NUMBER_OF_HENS_COLUMNS

EGG_COLUMNS = [
    "number_of_eggs_all",
    "number_of_eggs_from_hens_in_cages",
    "number_of_eggs_from_hens_cage_free",
    "number_of_eggs_from_hens_in_barns",
    "number_of_eggs_from_hens_free_range_not_organic",
    "number_of_eggs_from_hens_free_range_organic",
]


def _set_share_units(tb: Table) -> Table:
    for col in SHARE_OF_HENS_COLUMNS:
        if col in tb.columns:
            tb[col].metadata.unit = "%"
            tb[col].metadata.short_unit = "%"
    return tb


def prepare_uk_data(tb: Table) -> Table:
    """UK: approximate hen-housing shares from egg-throughput shares (assumes equal laying rates).

    Also keeps the original absolute egg and hen counts by housing type (UK-only columns).
    """
    tb = tb.copy()
    total = tb["number_of_eggs_all"]
    tb["number_of_eggs_from_hens_in_cages"] = tb["number_of_eggs_from_enriched_cages"]
    tb["number_of_eggs_from_hens_in_barns"] = tb["number_of_eggs_from_barns"]
    tb["number_of_eggs_from_hens_free_range_not_organic"] = tb["number_of_eggs_from_non_organic_free_range_farms"]
    tb["number_of_eggs_from_hens_free_range_organic"] = tb["number_of_eggs_from_organic_free_range_farms"]
    tb["number_of_eggs_from_hens_cage_free"] = total - tb["number_of_eggs_from_hens_in_cages"]
    tb["share_of_hens_in_cages"] = 100 * tb["number_of_eggs_from_hens_in_cages"] / total
    tb["share_of_hens_in_barns"] = 100 * tb["number_of_eggs_from_hens_in_barns"] / total
    tb["share_of_hens_free_range_not_organic"] = 100 * tb["number_of_eggs_from_hens_free_range_not_organic"] / total
    tb["share_of_hens_free_range_organic"] = 100 * tb["number_of_eggs_from_hens_free_range_organic"] / total
    tb["share_of_hens_cage_free"] = 100 - tb["share_of_hens_in_cages"]
    tb["number_of_hens_free_range_not_organic"] = tb["number_of_hens_free_range"]
    tb["number_of_hens_free_range_organic"] = tb["number_of_hens_organic"]
    tb["number_of_hens_all"] = tb["number_of_hens_in_cages"] + tb["number_of_hens_cage_free"]
    tb = _set_share_units(tb)
    return tb[HEN_COLUMNS + EGG_COLUMNS]


def prepare_us_data(tb: Table) -> Table:
    """US: compute hen-housing shares from USDA flock sizes.

    USDA reports a full breakdown by housing system from 2012 onwards. We map the USDA categories
    onto the shared schema as follows:
      - cages = caged
      - barn = non_organic_barn_aviary + organic
      - free-range non-organic = non_organic_free_range + non_organic_pastured
      - free-range organic = organic_free_range + organic_pastured

    For 2007-2011, USDA only reports binary cage vs cage-free, so all granular columns are left as NA.
    """
    tb = tb.copy()
    tb["number_of_hens_in_cages"] = tb["caged"]
    tb["number_of_hens_cage_free"] = tb["cage_free"]
    tb["number_of_hens_all"] = tb["number_of_hens_in_cages"] + tb["number_of_hens_cage_free"]

    tb["share_of_hens_in_cages"] = 100 * tb["number_of_hens_in_cages"] / tb["number_of_hens_all"]
    tb["share_of_hens_cage_free"] = 100 - tb["share_of_hens_in_cages"]
    tb["number_of_hens_in_barns"] = tb["non_organic_barn_aviary"] + tb["organic"]
    tb["number_of_hens_free_range_not_organic"] = tb["non_organic_free_range"] + tb["non_organic_pastured"]
    tb["number_of_hens_free_range_organic"] = tb["organic_free_range"] + tb["organic_pastured"]
    tb["share_of_hens_in_barns"] = 100 * tb["number_of_hens_in_barns"] / tb["number_of_hens_all"]
    tb["share_of_hens_free_range_not_organic"] = (
        100 * tb["number_of_hens_free_range_not_organic"] / tb["number_of_hens_all"]
    )
    tb["share_of_hens_free_range_organic"] = 100 * tb["number_of_hens_free_range_organic"] / tb["number_of_hens_all"]

    tb = _set_share_units(tb)
    return tb[HEN_COLUMNS]


def prepare_eu_data(tb: Table) -> Table:
    """EU: compute hen-housing shares from European Commission flock data.

    Conventional cages were banned EU-wide in 2012, so cage counts here are enriched cages only.
    """
    tb = tb.copy()
    tb["number_of_hens_all"] = tb["total"]
    tb["number_of_hens_in_cages"] = tb["enriched_cage"]
    tb["number_of_hens_in_barns"] = tb["barn"]
    tb["number_of_hens_free_range_not_organic"] = tb["free_range"]
    tb["number_of_hens_free_range_organic"] = tb["organic"]
    tb["number_of_hens_cage_free"] = (
        tb["number_of_hens_in_barns"]
        + tb["number_of_hens_free_range_not_organic"]
        + tb["number_of_hens_free_range_organic"]
    )
    tb["share_of_hens_cage_free"] = 100 * tb["number_of_hens_cage_free"] / tb["total"]
    tb["share_of_hens_in_cages"] = 100 - tb["share_of_hens_cage_free"]
    tb["share_of_hens_in_barns"] = 100 * tb["barn"] / tb["total"]
    tb["share_of_hens_free_range_not_organic"] = 100 * tb["free_range"] / tb["total"]
    tb["share_of_hens_free_range_organic"] = 100 * tb["organic"] / tb["total"]
    tb = _set_share_units(tb)
    return tb[HEN_COLUMNS]


def prepare_wfi_data(tb: Table) -> Table:
    """Welfare Footprint Institute: compute the cage-free share from the granular breakdown."""
    tb = tb.copy()
    tb["share_of_hens_cage_free"] = (
        tb["share_of_hens_in_barns"]
        + tb["share_of_hens_free_range_not_organic"]
        + tb["share_of_hens_free_range_organic"]
    )
    tb["number_of_hens_in_barns"] = tb["number_of_laying_hens"] * tb["share_of_hens_in_barns"] / 100
    tb["number_of_hens_free_range_not_organic"] = (
        tb["number_of_laying_hens"] * tb["share_of_hens_free_range_not_organic"] / 100
    )
    tb["number_of_hens_free_range_organic"] = tb["number_of_laying_hens"] * tb["share_of_hens_free_range_organic"] / 100
    number_of_hens_cage_free = (
        tb["number_of_hens_in_barns"]
        + tb["number_of_hens_free_range_not_organic"]
        + tb["number_of_hens_free_range_organic"]
    )
    tb["number_of_hens_cage_free"] = tb["number_of_hens_cage_free"].fillna(number_of_hens_cage_free)
    tb["number_of_hens_all"] = tb["number_of_laying_hens"]
    tb = _set_share_units(tb)
    return tb[HEN_COLUMNS]


def run() -> None:
    #
    # Load inputs.
    #
    ds_uk = paths.load_dataset("uk_egg_statistics")
    tb_uk = ds_uk.read("uk_egg_statistics")

    ds_us = paths.load_dataset("us_egg_production")
    tb_us = ds_us.read("us_egg_production")

    ds_eu = paths.load_dataset("laying_hens_keeping_eu")
    tb_eu = ds_eu.read("laying_hens_keeping_eu")

    ds_wfi = paths.load_dataset("global_hen_inventory")
    tb_wfi = ds_wfi.read("global_hen_inventory")

    #
    # Process data.
    #
    tb_uk = prepare_uk_data(tb_uk)
    tb_us = prepare_us_data(tb_us)
    tb_eu = prepare_eu_data(tb_eu)
    tb_wfi = prepare_wfi_data(tb_wfi)

    # Keep UK egg-production indicators in a separate table from multi-country hen indicators.
    tb_eggs = tb_uk[["country", "year"] + EGG_COLUMNS]

    # Combine sources, taking the first non-null value per column for each (country, year). The European Commission data is listed first (so it appears first in inherited origins) because it covers most of the data; UK and US follow; and WFI fills in the rest. None of these sources overlap on (country, year) pairs, so the order only affects the order of origins in metadata, not the values.
    tb_hens = pr.concat([tb_eu, tb_uk[HEN_COLUMNS], tb_us, tb_wfi], ignore_index=True, short_name="hen_statistics")
    tb_hens = tb_hens.groupby(["country", "year"], as_index=False, observed=True).first()

    # Improve table format.
    tb_eggs = tb_eggs.format(short_name="egg_statistics")
    tb_hens = tb_hens.format(short_name="hen_statistics")

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb_eggs, tb_hens])
    ds_garden.save()
