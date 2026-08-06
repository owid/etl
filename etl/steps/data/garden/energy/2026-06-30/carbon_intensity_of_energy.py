"""Carbon intensity of energy: CO2 emissions per unit of total energy supply.

Combines fossil CO2 emissions (Global Carbon Budget) with total energy supply (energy_mix, TES),
so that the carbon intensity of energy is expressed on the same Total Energy Supply basis as the rest
of the 2026 energy datasets.

This is the TES-based successor to the Global Carbon Budget's own
``emissions_total_per_unit_energy``, which still divides by the old substitution-method primary energy.

NOTE: This step loads the Global Carbon Budget only for its emissions numerator, so the old
substitution-method primary energy that the Global Carbon Budget depends on never touches these values.
Once the Global Carbon Budget itself moves to Total Energy Supply (in the emissions follow-up), the
Global Carbon Budget must *stop depending on energy* (not be repointed to energy_mix): this step depends
on the Global Carbon Budget, so making the Global Carbon Budget depend on energy_mix would create a cycle.
"""

import numpy as np
from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Unit conversions, matching the Global Carbon Budget step so the intensity is on the same scale.
TONNES_OF_CO2_TO_G_OF_CO2 = 1e6
TWH_TO_KWH = 1e9


def run() -> None:
    #
    # Load inputs.
    #
    # Fossil CO2 emissions (numerator) from the Global Carbon Budget.
    ds_gcb = paths.load_dataset("global_carbon_budget")
    tb_gcb = ds_gcb.read("global_carbon_budget")[
        ["country", "year", "emissions_total", "emissions_total_including_land_use_change"]
    ]

    # Total energy supply (denominator) from the energy mix.
    ds_energy = paths.load_dataset("energy_mix")
    tb_energy = ds_energy.read("energy_mix")[["country", "year", "total_energy_supply_twh"]]

    #
    # Process data.
    #
    # Keep only country-years present in both sources (intensity is undefined otherwise).
    tb = pr.merge(tb_gcb, tb_energy, on=["country", "year"], how="inner")

    # Carbon intensity of energy, in grams of CO2 per kWh of total energy supply.
    tb["emissions_total_per_unit_energy"] = (
        TONNES_OF_CO2_TO_G_OF_CO2 * tb["emissions_total"] / (tb["total_energy_supply_twh"] * TWH_TO_KWH)
    )
    tb["emissions_total_including_land_use_change_per_unit_energy"] = (
        TONNES_OF_CO2_TO_G_OF_CO2
        * tb["emissions_total_including_land_use_change"]
        / (tb["total_energy_supply_twh"] * TWH_TO_KWH)
    )

    # Where total energy supply is zero, the division yields infinities; treat those as missing.
    for column in ["emissions_total_per_unit_energy", "emissions_total_including_land_use_change_per_unit_energy"]:
        tb.loc[~np.isfinite(tb[column].astype("float64")), column] = np.nan

    tb = tb.drop(columns=["emissions_total", "emissions_total_including_land_use_change", "total_energy_supply_twh"])

    # Derived indicators must not inherit key points from the GCB input; key points come only from
    # this step's own meta.yml.
    for column in tb.columns:
        tb[column].m.description_key = []

    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
