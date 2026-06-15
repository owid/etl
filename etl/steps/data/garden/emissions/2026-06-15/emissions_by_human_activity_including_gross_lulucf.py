"""Greenhouse gas emissions by human activity, using gross agriculture and land-use emissions.

This step takes ``emissions_by_human_activity`` (where the "Growing food" activity uses Climate Watch's
agriculture and land-use-change-and-forestry sectors) and replaces those emissions with gross
agriculture-and-land-use emissions derived from Jones et al. (national contributions).

Climate Watch's land-use-change-and-forestry emissions are *net* (so global land-use CO2 is close to
zero), which understates the climate impact of land use. Jones et al. report the *gross* land component
(land-use-change CO2 plus agricultural CH4 and N2O), which is much larger. We convert Jones' per-gas land
emissions to CO2 equivalents using IPCC AR4 100-year global warming potentials (CH4 = 25, N2O = 298),
rather than the AR6 values used in the national_contributions garden step, and use the result as the new
"Growing food" activity.

It produces two country-level tables (electricity is still its own activity, as in the input step):

1. ``emissions_by_human_activity_including_gross_lulucf``
2. ``emissions_by_human_activity_including_gross_lulucf_including_other``
"""

import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# IPCC AR4 100-year global warming potentials, used to convert Jones et al. land emissions to CO2 equivalents.
CH4_TO_CO2EQ_AR4 = 25
N2O_TO_CO2EQ_AR4 = 298

# Activity whose emissions we replace with Jones et al.'s gross agriculture-and-land-use figures.
ACTIVITY_TO_REPLACE = "Growing food"


def compute_jones_land(ds_jones):
    # Read Jones et al.'s per-gas land (LULUCF) emissions, already harmonized and in tonnes.
    tb = ds_jones.read("national_contributions")
    tb = tb[["country", "year", "annual_emissions_co2_land", "annual_emissions_ch4_land", "annual_emissions_n2o_land"]]

    # Gross land-use CO2 emissions (Jones' LULUCF CO2, which is much larger than Climate Watch's net figure).
    tb["co2_land"] = tb["annual_emissions_co2_land"]
    # Gross agriculture-and-land-use GHG emissions, using AR4 global warming potentials.
    tb["ghg_land"] = (
        tb["annual_emissions_co2_land"]
        + CH4_TO_CO2EQ_AR4 * tb["annual_emissions_ch4_land"]
        + N2O_TO_CO2EQ_AR4 * tb["annual_emissions_n2o_land"]
    )

    return tb[["country", "year", "co2_land", "ghg_land"]]


def compute_other_fuel_combustion(tb_base, tb_other):
    # In the base table "Growing food" = agriculture + land use + other fuel combustion; in the
    # including-other table it is agriculture + land use only. Their difference is other fuel combustion.
    gf_base = tb_base[tb_base["sector"] == ACTIVITY_TO_REPLACE][["country", "year", "co2_emissions", "ghg_emissions"]]
    gf_other = tb_other[tb_other["sector"] == ACTIVITY_TO_REPLACE][
        ["country", "year", "co2_emissions", "ghg_emissions"]
    ]
    ofc = gf_base.merge(gf_other, on=["country", "year"], how="inner", suffixes=("_base", "_other"))
    ofc["co2_ofc"] = ofc["co2_emissions_base"] - ofc["co2_emissions_other"]
    ofc["ghg_ofc"] = ofc["ghg_emissions_base"] - ofc["ghg_emissions_other"]

    return ofc[["country", "year", "co2_ofc", "ghg_ofc"]]


def replace_growing_food(tb, jones, ofc=None):
    # Keep every activity except the one we replace.
    tb_other = tb[tb["sector"] != ACTIVITY_TO_REPLACE].reset_index(drop=True)

    # Build the new "Growing food" activity from Jones' gross land emissions.
    gf = tb[tb["sector"] == ACTIVITY_TO_REPLACE].reset_index(drop=True)
    gf = gf.merge(jones, on=["country", "year"], how="left")
    # Use Jones' gross figures, falling back to Climate Watch where Jones has no data.
    gf["co2_emissions"] = gf["co2_land"].fillna(gf["co2_emissions"])
    gf["ghg_emissions"] = gf["ghg_land"].fillna(gf["ghg_emissions"])
    gf = gf.drop(columns=["co2_land", "ghg_land"], errors="raise")

    # In the base table, "Growing food" also contains other fuel combustion, which we keep on top.
    if ofc is not None:
        gf = gf.merge(ofc, on=["country", "year"], how="left")
        with pr.ignore_warnings():
            gf["co2_emissions"] = gf["co2_emissions"] + gf["co2_ofc"].fillna(0)
            gf["ghg_emissions"] = gf["ghg_emissions"] + gf["ghg_ofc"].fillna(0)
        gf = gf.drop(columns=["co2_ofc", "ghg_ofc"], errors="raise")

    tb = pr.concat([tb_other, gf], ignore_index=True)

    return tb


def sanity_check_outputs(tb_base, tb_other, jones):
    # Every input activity should still be present.
    error = "Set of activities changed unexpectedly."
    assert set(tb_base["sector"]) == {
        "Growing food",
        "Getting around",
        "Keeping warm and cool",
        "Electricity",
        "Making things",
    }, error
    assert set(tb_other["sector"]) == {
        "Growing food",
        "Getting around",
        "Keeping warm and cool",
        "Electricity",
        "Making things",
        "Other",
    }, error

    # Gross land use should be substantially larger than Climate Watch's net figure: at World level, the
    # gross "Growing food" GHG (including-other table, which is exactly agriculture + land use) should
    # clearly exceed Jones' land CO2 alone and be well above zero.
    world_gf = tb_other[(tb_other["country"] == "World") & (tb_other["sector"] == "Growing food")]
    error = "Expected positive gross 'Growing food' emissions at World level."
    assert (world_gf["ghg_emissions"] > 0).all(), error

    # Warn about entities that have a "Growing food" activity but no Jones coverage (kept as Climate Watch).
    covered = set(zip(jones.dropna(subset=["ghg_land"])["country"], jones.dropna(subset=["ghg_land"])["year"]))
    gf = tb_other[tb_other["sector"] == "Growing food"]
    missing = sorted(set(gf["country"]) - {c for c, _ in covered})
    if missing:
        paths.log.warning(f"Entities without Jones land coverage (kept as Climate Watch): {missing}")


def run() -> None:
    #
    # Load inputs.
    #
    # Load the base step (electricity as its own activity) and read both tables.
    ds_step1 = paths.load_dataset("emissions_by_human_activity")
    tb_base = ds_step1.read("emissions_by_human_activity")
    tb_other = ds_step1.read("emissions_by_human_activity_including_other")

    # Load Jones et al. national contributions (harmonized garden, with per-gas land emissions in tonnes).
    ds_jones = paths.load_dataset("national_contributions")

    #
    # Process data.
    #
    # Gross agriculture-and-land-use emissions from Jones et al., using AR4 global warming potentials.
    jones = compute_jones_land(ds_jones)

    # Other fuel combustion (the part of the base "Growing food" that is not agriculture or land use).
    ofc = compute_other_fuel_combustion(tb_base=tb_base, tb_other=tb_other)

    # Replace "Growing food" with the gross figures in each table.
    tb_base_gross = replace_growing_food(tb_base, jones, ofc=ofc)
    tb_other_gross = replace_growing_food(tb_other, jones, ofc=None)

    # Sanity checks.
    sanity_check_outputs(tb_base=tb_base_gross, tb_other=tb_other_gross, jones=jones)

    # Improve table format.
    tb_base_gross = tb_base_gross.format(
        keys=["country", "year", "sector"], short_name="emissions_by_human_activity_including_gross_lulucf"
    )
    tb_other_gross = tb_other_gross.format(
        keys=["country", "year", "sector"],
        short_name="emissions_by_human_activity_including_gross_lulucf_including_other",
    )

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb_base_gross, tb_other_gross])
    ds_garden.save()
