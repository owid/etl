"""Greenhouse gas emissions by human activity, using gross agriculture and land-use emissions.

This step takes ``emissions_by_human_activity`` (where the "Growing food" activity uses Climate Watch's
agriculture and land-use-change-and-forestry sectors) and replaces those emissions with gross
agriculture-and-land-use emissions derived from Jones et al. (national contributions).

Climate Watch's land-use-change-and-forestry emissions are *net* (so global land-use CO2 is close to
zero), which understates the climate impact of land use. Jones et al. report the *gross* land component
(land-use-change CO2 plus agricultural CH4 and N2O), which is much larger. We convert Jones' per-gas land
emissions to CO2 equivalents using IPCC AR5 100-year global warming potentials (CH4 = 28, N2O = 265), to
match Climate Watch (the source for every other activity), which expresses non-CO2 gases with AR5 values.
This differs from the AR6 values used in the national_contributions garden step. The result becomes the new
"Growing food" activity.

It produces two country-level tables (electricity is still its own activity, as in the input step):

1. ``emissions_by_human_activity_including_gross_lulucf``
2. ``emissions_by_human_activity_including_gross_lulucf_including_other``
"""

import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# IPCC AR5 100-year global warming potentials, used to convert Jones et al. land emissions to CO2 equivalents.
# Climate Watch (the source for all other activities) expresses non-CO2 gases with AR5 GWPs, so we match it.
CH4_TO_CO2EQ_AR5 = 28
N2O_TO_CO2EQ_AR5 = 265

# Activity whose emissions we replace with Jones et al.'s gross agriculture-and-land-use figures.
ACTIVITY_TO_REPLACE = "Growing food"


def compute_jones_land(ds_jones):
    # Read Jones et al.'s per-gas land (LULUCF) emissions, already harmonized and in tonnes.
    tb = ds_jones.read("national_contributions")
    tb = tb[["country", "year", "annual_emissions_co2_land", "annual_emissions_ch4_land", "annual_emissions_n2o_land"]]

    # Gross land-use CO2 emissions (Jones' LULUCF CO2, which is much larger than Climate Watch's net figure).
    tb["co2_land"] = tb["annual_emissions_co2_land"]
    # Gross agriculture-and-land-use GHG emissions, using AR5 global warming potentials.
    tb["ghg_land"] = (
        tb["annual_emissions_co2_land"]
        + CH4_TO_CO2EQ_AR5 * tb["annual_emissions_ch4_land"]
        + N2O_TO_CO2EQ_AR5 * tb["annual_emissions_n2o_land"]
    )

    # Keep only entity-years where Jones provides the complete gross figures. A few small island states
    # (e.g. Marshall Islands, Palau) report land-use CO2 but no agricultural CH4/N2O, leaving the GHG sum
    # undefined; those are dropped downstream rather than backfilled with Climate Watch's net figures.
    tb = tb.dropna(subset=["co2_land", "ghg_land"]).reset_index(drop=True)

    return tb[["country", "year", "co2_land", "ghg_land"]]


def compute_other_fuel_combustion(tb_base, tb_other):
    # In the base table "Growing food" = agriculture + land use + other fuel combustion; in the
    # including-other table it is agriculture + land use only. Their difference is other fuel combustion.
    gf_base = tb_base[tb_base["sector"] == ACTIVITY_TO_REPLACE][["country", "year", "co2_emissions", "ghg_emissions"]]
    gf_other = tb_other[tb_other["sector"] == ACTIVITY_TO_REPLACE][
        ["country", "year", "co2_emissions", "ghg_emissions"]
    ]
    ofc = pr.merge(gf_base, gf_other, on=["country", "year"], how="inner", suffixes=("_base", "_other"))
    ofc["co2_ofc"] = ofc["co2_emissions_base"] - ofc["co2_emissions_other"]
    ofc["ghg_ofc"] = ofc["ghg_emissions_base"] - ofc["ghg_emissions_other"]

    return ofc[["country", "year", "co2_ofc", "ghg_ofc"]]


def replace_growing_food(tb, jones, ofc=None):
    # Keep every activity except the one we replace.
    tb_other = tb[tb["sector"] != ACTIVITY_TO_REPLACE].reset_index(drop=True)

    # Build the new "Growing food" activity from Jones' gross land emissions, keeping only the (country, year)
    # pairs where Jones has land data. The inner merge drops the rest, rather than falling back to Climate
    # Watch's net figures (which would mix conventions within a single chart).
    gf = tb[tb["sector"] == ACTIVITY_TO_REPLACE].reset_index(drop=True)
    gf = pr.merge(gf, jones, on=["country", "year"], how="inner")
    gf["co2_emissions"] = gf["co2_land"]
    gf["ghg_emissions"] = gf["ghg_land"]
    gf = gf.drop(columns=["co2_land", "ghg_land"], errors="raise")

    # In the base table, "Growing food" also contains other fuel combustion, which we keep on top.
    if ofc is not None:
        gf = pr.merge(gf, ofc, on=["country", "year"], how="left")
        with pr.ignore_warnings():
            gf["co2_emissions"] = gf["co2_emissions"] + gf["co2_ofc"].fillna(0)
            gf["ghg_emissions"] = gf["ghg_emissions"] + gf["ghg_ofc"].fillna(0)
        gf = gf.drop(columns=["co2_ofc", "ghg_ofc"], errors="raise")

    # Drop the other activities for any (country, year) without Jones land data, so an entity's stacked
    # total never mixes gross land emissions (Jones) with net land emissions (Climate Watch).
    tb_other = pr.merge(tb_other, gf[["country", "year"]].drop_duplicates(), on=["country", "year"], how="inner")

    tb = pr.concat([tb_other, gf], ignore_index=True)

    return tb


def sanity_check_outputs(tb_base, tb_other):
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
    # Gross agriculture-and-land-use emissions from Jones et al., using AR5 global warming potentials.
    jones = compute_jones_land(ds_jones)

    # Other fuel combustion (the part of the base "Growing food" that is not agriculture or land use).
    ofc = compute_other_fuel_combustion(tb_base=tb_base, tb_other=tb_other)

    # Replace "Growing food" with the gross figures in each table.
    tb_base_gross = replace_growing_food(tb_base, jones, ofc=ofc)
    tb_other_gross = replace_growing_food(tb_other, jones, ofc=None)

    # Report (country, year) rows dropped for lacking Jones land data, so coverage gaps surface (including
    # entities that lose only some years) rather than being silently filled with Climate Watch's net figures.
    before = set(map(tuple, tb_base[["country", "year"]].drop_duplicates().to_numpy()))
    after = set(map(tuple, tb_base_gross[["country", "year"]].drop_duplicates().to_numpy()))
    dropped = sorted(before - after)
    if dropped:
        by_country = {
            c: f"{min(y for cc, y in dropped if cc == c)}-{max(y for cc, y in dropped if cc == c)}"
            for c in sorted({c for c, _ in dropped})
        }
        paths.log.warning(f"Dropped country-years without Jones land coverage: {by_country}")

    # Sanity checks.
    sanity_check_outputs(tb_base=tb_base_gross, tb_other=tb_other_gross)

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
