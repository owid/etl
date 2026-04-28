"""Combine hen housing-system data from multiple sources to give global coverage.

Modern time series:
  - UK: derived from Defra egg-throughput statistics, used as a proxy for hen housing shares.
        UK rows also carry absolute egg counts and estimated hen counts by housing type.
  - US: derived from USDA flock sizes (caged vs cage-free).
  - EU member states: derived from European Commission flock data (cages, barn, free-range, organic).

Single-year (~2019) values for ~75 additional countries come from the Welfare Footprint Institute
(formerly the Welfare Footprint Project) global hen inventory compilation.

For overlapping (country, year) pairs the modern time series take precedence over the WFI compilation.
"""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder

paths = PathFinder(__file__)

CORE_COLUMNS = [
    "country",
    "year",
    "share_of_hens_in_cages",
    "share_of_hens_cage_free",
    "number_of_hens_in_cages",
    "number_of_hens_cage_free",
]

UK_EXTRA_COLUMNS = [
    "number_of_eggs_from_enriched_cages",
    "number_of_eggs_from_barns",
    "number_of_eggs_from_non_organic_free_range_farms",
    "number_of_eggs_from_organic_free_range_farms",
    "number_of_eggs_all",
    "number_of_hens_in_barns",
    "number_of_hens_free_range",
    "number_of_hens_organic",
]


def _set_share_units(tb: Table) -> Table:
    for col in ["share_of_hens_in_cages", "share_of_hens_cage_free"]:
        tb[col].metadata.unit = "%"
        tb[col].metadata.short_unit = "%"
    return tb


def prepare_uk_data(tb: Table) -> Table:
    """UK: approximate hen-housing shares from egg-throughput shares (assumes equal laying rates).

    Also keeps the original absolute egg and hen counts by housing type (UK-only columns).
    """
    tb = tb.copy()
    tb["share_of_hens_in_cages"] = 100 * tb["number_of_eggs_from_enriched_cages"] / tb["number_of_eggs_all"]
    tb["share_of_hens_cage_free"] = 100 - tb["share_of_hens_in_cages"]
    tb = _set_share_units(tb)
    return tb[CORE_COLUMNS + UK_EXTRA_COLUMNS]


def prepare_us_data(tb: Table) -> Table:
    """US: compute hen-housing shares directly from USDA flock sizes."""
    tb = tb.copy()
    tb["number_of_hens_in_cages"] = tb["caged"]
    tb["number_of_hens_cage_free"] = tb["cage_free"]
    total_hens = tb["number_of_hens_in_cages"] + tb["number_of_hens_cage_free"]
    tb["share_of_hens_cage_free"] = 100 * tb["number_of_hens_cage_free"] / total_hens
    tb["share_of_hens_in_cages"] = 100 - tb["share_of_hens_cage_free"]
    tb = _set_share_units(tb)
    return tb[CORE_COLUMNS]


def prepare_eu_data(tb: Table) -> Table:
    """EU: compute hen-housing shares from European Commission flock data.

    Conventional cages were banned EU-wide in 2012, so cage counts here are enriched cages only.
    """
    tb = tb.copy()
    tb["number_of_hens_in_cages"] = tb["enriched_cage"]
    tb["number_of_hens_cage_free"] = tb["barn"] + tb["free_range"] + tb["organic"]
    tb["share_of_hens_cage_free"] = 100 * tb["number_of_hens_cage_free"] / tb["total"]
    tb["share_of_hens_in_cages"] = 100 - tb["share_of_hens_cage_free"]
    tb = _set_share_units(tb)
    return tb[CORE_COLUMNS]


def prepare_wfi_data(tb: Table) -> Table:
    """Welfare Footprint Institute: compute the cage-free share from the housing breakdown.

    Sum barn + non-organic free-range + organic free-range, instead of using `100 - cages`,
    so that the "unknown housing" share is not implicitly counted as cage-free.
    """
    tb = tb.copy()
    tb["share_of_hens_cage_free"] = (
        tb["share_of_hens_in_barns"]
        + tb["share_of_hens_free_range_not_organic"]
        + tb["share_of_hens_free_range_organic"]
    )
    tb = _set_share_units(tb)
    return tb[CORE_COLUMNS]


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

    # Combine sources, preferring modern time series for overlapping (country, year) pairs.
    tb = pr.concat([tb_uk, tb_us, tb_eu, tb_wfi], ignore_index=True, short_name=paths.short_name)
    tb = tb.drop_duplicates(subset=["country", "year"], keep="first")

    # Improve table format.
    tb = tb.format()

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
