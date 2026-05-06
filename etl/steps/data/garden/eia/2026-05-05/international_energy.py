"""Garden step for EIA International Energy data.

Pivots the long-format meadow table to a wide table of curated indicators (energy and electricity
in TWh, installed capacity in GW, CO2 in million tonnes, etc.), harmonizes country names, and
adds OWID region aggregates.
"""

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Conversion factors.
TJ_TO_TWH = 1 / 3600
QUAD_BTU_TO_TWH = 293.0710701722222  # 1 quadrillion Btu = 293.07 TWh.
MILLION_BTU_TO_KWH = 293.0710701722222
THOUSAND_BTU_TO_KWH = 0.2930710701722222
SHORT_TON_TO_TONNE = 0.9071847  # 1 short ton = 0.907 metric tonnes.

# Curated indicators: (eia_variable, eia_unit, output_column, factor, output_unit, output_short_unit, title).
# The pair (eia_variable, eia_unit) selects one row family in the meadow long table; the value is
# multiplied by `factor` and written to the output column.
INDICATORS: list[tuple[str, str, str, float, str, str, str]] = [
    # Total energy.
    (
        "Total energy consumption",
        "terajoules",
        "total_energy_consumption",
        TJ_TO_TWH,
        "terawatt-hours",
        "TWh",
        "Total energy consumption",
    ),
    (
        "Total energy production",
        "terajoules",
        "total_energy_production",
        TJ_TO_TWH,
        "terawatt-hours",
        "TWh",
        "Total energy production",
    ),
    # Energy consumption by source.
    (
        "Total energy consumption from coal",
        "quadrillion Btu",
        "energy_consumption_from_coal",
        QUAD_BTU_TO_TWH,
        "terawatt-hours",
        "TWh",
        "Energy consumption from coal",
    ),
    (
        "Total energy consumption from natural gas",
        "quadrillion Btu",
        "energy_consumption_from_natural_gas",
        QUAD_BTU_TO_TWH,
        "terawatt-hours",
        "TWh",
        "Energy consumption from natural gas",
    ),
    (
        "Total energy consumption from petroleum and other liquids",
        "quadrillion Btu",
        "energy_consumption_from_petroleum",
        QUAD_BTU_TO_TWH,
        "terawatt-hours",
        "TWh",
        "Energy consumption from petroleum and other liquids",
    ),
    (
        "Total energy consumption from nuclear",
        "quadrillion Btu",
        "energy_consumption_from_nuclear",
        QUAD_BTU_TO_TWH,
        "terawatt-hours",
        "TWh",
        "Energy consumption from nuclear",
    ),
    (
        "Total energy consumption from renewables and other",
        "quadrillion Btu",
        "energy_consumption_from_renewables",
        QUAD_BTU_TO_TWH,
        "terawatt-hours",
        "TWh",
        "Energy consumption from renewables and other",
    ),
    # Energy production by source.
    (
        "Total energy production from coal",
        "quadrillion Btu",
        "energy_production_from_coal",
        QUAD_BTU_TO_TWH,
        "terawatt-hours",
        "TWh",
        "Energy production from coal",
    ),
    (
        "Total energy production from natural gas",
        "quadrillion Btu",
        "energy_production_from_natural_gas",
        QUAD_BTU_TO_TWH,
        "terawatt-hours",
        "TWh",
        "Energy production from natural gas",
    ),
    (
        "Total energy production from petroleum and other liquids",
        "quadrillion Btu",
        "energy_production_from_petroleum",
        QUAD_BTU_TO_TWH,
        "terawatt-hours",
        "TWh",
        "Energy production from petroleum and other liquids",
    ),
    (
        "Total energy production from nuclear",
        "quadrillion Btu",
        "energy_production_from_nuclear",
        QUAD_BTU_TO_TWH,
        "terawatt-hours",
        "TWh",
        "Energy production from nuclear",
    ),
    (
        "Total energy production from renewables and other",
        "quadrillion Btu",
        "energy_production_from_renewables",
        QUAD_BTU_TO_TWH,
        "terawatt-hours",
        "TWh",
        "Energy production from renewables and other",
    ),
    # Coal.
    ("Coal consumption", "terajoules", "coal_consumption", TJ_TO_TWH, "terawatt-hours", "TWh", "Coal consumption"),
    ("Coal production", "terajoules", "coal_production", TJ_TO_TWH, "terawatt-hours", "TWh", "Coal production"),
    ("Coal imports", "terajoules", "coal_imports", TJ_TO_TWH, "terawatt-hours", "TWh", "Coal imports"),
    ("Coal exports", "terajoules", "coal_exports", TJ_TO_TWH, "terawatt-hours", "TWh", "Coal exports"),
    (
        "Coal reserves",
        "million short tons",
        "coal_reserves",
        SHORT_TON_TO_TONNE,
        "million tonnes",
        "Mt",
        "Coal reserves",
    ),
    # Natural gas.
    (
        "Dry natural gas consumption",
        "billion cubic meters",
        "natural_gas_consumption",
        1.0,
        "billion cubic metres",
        "bcm",
        "Natural gas consumption",
    ),
    (
        "Dry natural gas production",
        "billion cubic meters",
        "natural_gas_production",
        1.0,
        "billion cubic metres",
        "bcm",
        "Natural gas production",
    ),
    (
        "Dry natural gas imports",
        "billion cubic meters",
        "natural_gas_imports",
        1.0,
        "billion cubic metres",
        "bcm",
        "Natural gas imports",
    ),
    (
        "Dry natural gas exports",
        "billion cubic meters",
        "natural_gas_exports",
        1.0,
        "billion cubic metres",
        "bcm",
        "Natural gas exports",
    ),
    # Petroleum and oil.
    (
        "Petroleum and other liquids consumption",
        "thousand barrels per day",
        "petroleum_consumption",
        1.0,
        "thousand barrels per day",
        "kb/d",
        "Petroleum and other liquids consumption",
    ),
    (
        "Petroleum and other liquids production",
        "thousand barrels per day",
        "petroleum_production",
        1.0,
        "thousand barrels per day",
        "kb/d",
        "Petroleum and other liquids production",
    ),
    (
        "Crude oil including lease condensate production",
        "thousand barrels per day",
        "crude_oil_production",
        1.0,
        "thousand barrels per day",
        "kb/d",
        "Crude oil production (including lease condensate)",
    ),
    # Electricity flows.
    (
        "Electricity net generation",
        "billion kilowatthours",
        "electricity_generation",
        1.0,
        "terawatt-hours",
        "TWh",
        "Electricity generation",
    ),
    (
        "Electricity net consumption",
        "billion kilowatthours",
        "electricity_consumption",
        1.0,
        "terawatt-hours",
        "TWh",
        "Electricity consumption",
    ),
    (
        "Electricity imports",
        "billion kilowatthours",
        "electricity_imports",
        1.0,
        "terawatt-hours",
        "TWh",
        "Electricity imports",
    ),
    (
        "Electricity exports",
        "billion kilowatthours",
        "electricity_exports",
        1.0,
        "terawatt-hours",
        "TWh",
        "Electricity exports",
    ),
    (
        "Electricity net imports",
        "billion kilowatthours",
        "electricity_net_imports",
        1.0,
        "terawatt-hours",
        "TWh",
        "Electricity net imports",
    ),
    (
        "Electricity distribution losses",
        "billion kilowatthours",
        "electricity_distribution_losses",
        1.0,
        "terawatt-hours",
        "TWh",
        "Electricity distribution losses",
    ),
    (
        "Electricity installed capacity",
        "million kilowatts",
        "electricity_installed_capacity",
        1.0,
        "gigawatts",
        "GW",
        "Electricity installed capacity",
    ),
    # Electricity by source — generation.
    (
        "Hydroelectricity net generation",
        "billion kilowatthours",
        "electricity_from_hydro",
        1.0,
        "terawatt-hours",
        "TWh",
        "Electricity generation from hydro",
    ),
    (
        "Nuclear electricity net generation",
        "billion kilowatthours",
        "electricity_from_nuclear",
        1.0,
        "terawatt-hours",
        "TWh",
        "Electricity generation from nuclear",
    ),
    (
        "Solar electricity net generation",
        "billion kilowatthours",
        "electricity_from_solar",
        1.0,
        "terawatt-hours",
        "TWh",
        "Electricity generation from solar",
    ),
    (
        "Wind electricity net generation",
        "billion kilowatthours",
        "electricity_from_wind",
        1.0,
        "terawatt-hours",
        "TWh",
        "Electricity generation from wind",
    ),
    (
        "Geothermal electricity net generation",
        "billion kilowatthours",
        "electricity_from_geothermal",
        1.0,
        "terawatt-hours",
        "TWh",
        "Electricity generation from geothermal",
    ),
    (
        "Biomass and waste electricity net generation",
        "billion kilowatthours",
        "electricity_from_biomass",
        1.0,
        "terawatt-hours",
        "TWh",
        "Electricity generation from biomass and waste",
    ),
    (
        "Tide and wave electricity net generation",
        "billion kilowatthours",
        "electricity_from_tide_and_wave",
        1.0,
        "terawatt-hours",
        "TWh",
        "Electricity generation from tide and wave",
    ),
    (
        "Renewable electricity net generation",
        "billion kilowatthours",
        "electricity_from_renewables",
        1.0,
        "terawatt-hours",
        "TWh",
        "Electricity generation from renewables",
    ),
    (
        "Non-hydro renewable electricity net generation",
        "billion kilowatthours",
        "electricity_from_non_hydro_renewables",
        1.0,
        "terawatt-hours",
        "TWh",
        "Electricity generation from non-hydro renewables",
    ),
    (
        "Fossil fuels electricity net generation",
        "billion kilowatthours",
        "electricity_from_fossil_fuels",
        1.0,
        "terawatt-hours",
        "TWh",
        "Electricity generation from fossil fuels",
    ),
    # Electricity by source — installed capacity.
    (
        "Hydroelectricity installed capacity",
        "million kilowatts",
        "installed_capacity_hydro",
        1.0,
        "gigawatts",
        "GW",
        "Installed hydro capacity",
    ),
    (
        "Nuclear electricity installed capacity",
        "million kilowatts",
        "installed_capacity_nuclear",
        1.0,
        "gigawatts",
        "GW",
        "Installed nuclear capacity",
    ),
    (
        "Solar electricity installed capacity",
        "million kilowatts",
        "installed_capacity_solar",
        1.0,
        "gigawatts",
        "GW",
        "Installed solar capacity",
    ),
    (
        "Wind electricity installed capacity",
        "million kilowatts",
        "installed_capacity_wind",
        1.0,
        "gigawatts",
        "GW",
        "Installed wind capacity",
    ),
    (
        "Geothermal electricity installed capacity",
        "million kilowatts",
        "installed_capacity_geothermal",
        1.0,
        "gigawatts",
        "GW",
        "Installed geothermal capacity",
    ),
    (
        "Biomass and waste electricity installed capacity",
        "million kilowatts",
        "installed_capacity_biomass",
        1.0,
        "gigawatts",
        "GW",
        "Installed biomass and waste capacity",
    ),
    (
        "Renewable electricity installed capacity",
        "million kilowatts",
        "installed_capacity_renewables",
        1.0,
        "gigawatts",
        "GW",
        "Installed renewable capacity",
    ),
    (
        "Non-hydro renewable electricity installed capacity",
        "million kilowatts",
        "installed_capacity_non_hydro_renewables",
        1.0,
        "gigawatts",
        "GW",
        "Installed non-hydro renewable capacity",
    ),
    (
        "Fossil fuels electricity installed capacity",
        "million kilowatts",
        "installed_capacity_fossil_fuels",
        1.0,
        "gigawatts",
        "GW",
        "Installed fossil-fuel capacity",
    ),
    # CO2 emissions from energy.
    (
        "CO2 emissions",
        "million metric tonnes carbon dioxide",
        "co2_emissions",
        1.0,
        "million tonnes",
        "Mt",
        "CO2 emissions from energy",
    ),
    (
        "Coal and coke CO2 emissions",
        "million metric tonnes carbon dioxide",
        "co2_emissions_from_coal",
        1.0,
        "million tonnes",
        "Mt",
        "CO2 emissions from coal and coke",
    ),
    (
        "Consumed natural gas CO2 emissions",
        "million metric tonnes carbon dioxide",
        "co2_emissions_from_natural_gas",
        1.0,
        "million tonnes",
        "Mt",
        "CO2 emissions from natural gas consumption",
    ),
    (
        "Petroleum and other liquids CO2 emissions",
        "million metric tonnes carbon dioxide",
        "co2_emissions_from_petroleum",
        1.0,
        "million tonnes",
        "Mt",
        "CO2 emissions from petroleum and other liquids",
    ),
    # Per capita / per GDP — intensive, not aggregated by region.
    (
        "Energy consumption per capita",
        "million Btu per person",
        "energy_consumption_per_capita",
        MILLION_BTU_TO_KWH,
        "kilowatt-hours per person",
        "kWh",
        "Energy consumption per person",
    ),
    (
        "Energy consumption per GDP",
        "thousand Btu per USD at purchasing power parities",
        "energy_intensity",
        THOUSAND_BTU_TO_KWH,
        "kilowatt-hours per international-$",
        "kWh",
        "Energy intensity (energy consumption per unit of GDP)",
    ),
    # Population and GDP.
    ("Population", "people in thousands", "population", 1000.0, "people", "", "Population (EIA)"),
    (
        "GDP at purchasing power parities",
        "billion dollars at purchasing power parities",
        "gdp_ppp",
        1.0,
        "billion international-$",
        "",
        "GDP at purchasing power parities (EIA)",
    ),
]

# Indicators that are intensive (per capita, per GDP) and shouldn't be summed across countries.
INTENSIVE_INDICATORS = {"energy_consumption_per_capita", "energy_intensity"}

# OWID region aggregates to create.
REGIONS = {
    "Africa": {},
    "Asia": {},
    "Europe": {},
    "North America": {},
    "Oceania": {},
    "South America": {},
    "Low-income countries": {},
    "Lower-middle-income countries": {},
    "Upper-middle-income countries": {},
    "High-income countries": {},
    "World": {},
}

# Known overlaps between historical regions and successor countries. Aruba's contribution is
# negligible compared to the Netherlands Antilles aggregate, so the small double counting in
# Europe-level aggregates does not affect results meaningfully.
KNOWN_OVERLAPS = [{year: {"Aruba", "Netherlands Antilles"} for year in range(1986, 2030)}]


def curate_indicators(tb_meadow: Table) -> Table:
    """Pivot the long-format meadow table into the wide curated indicator table."""
    selected_pairs = {(var, unit) for var, unit, *_ in INDICATORS}
    keep = [(v, u) in selected_pairs for v, u in zip(tb_meadow["variable"], tb_meadow["unit"])]
    tb = tb_meadow[keep][["country", "year", "variable", "unit", "value"]].copy()

    # Map (eia_variable, eia_unit) → (output_column, factor).
    pair_to_output = {(var, unit): (name, factor) for var, unit, name, factor, *_ in INDICATORS}

    # Apply unit conversions.
    factors = [pair_to_output[(v, u)][1] for v, u in zip(tb["variable"], tb["unit"])]
    tb["value"] = tb["value"] * factors

    # Replace (variable, unit) with the target output column name.
    tb["indicator"] = [pair_to_output[(v, u)][0] for v, u in zip(tb["variable"], tb["unit"])]
    tb = tb.drop(columns=["variable", "unit"])

    # Pivot to wide: one column per indicator.
    tb = tb.pivot(index=["country", "year"], columns="indicator", values="value").reset_index()
    tb.columns.name = None

    # Sort columns into the order defined in INDICATORS.
    ordered = ["country", "year"] + [name for *_unused, name, _f, _u, _su, _t in INDICATORS if name in tb.columns]
    return tb[ordered]


def attach_indicator_metadata(tb: Table) -> Table:
    """Set per-indicator title/unit/short_unit on the wide table.

    YAML metadata can override these later.
    """
    for _var, _unit, name, _factor, output_unit, output_short_unit, title in INDICATORS:
        if name not in tb.columns:
            continue
        tb[name].metadata.title = title
        tb[name].metadata.unit = output_unit
        tb[name].metadata.short_unit = output_short_unit
    return tb


def run() -> None:
    #
    # Load data.
    #
    ds_meadow = paths.load_dataset("international_energy")
    tb_meadow = ds_meadow.read("international_energy", safe_types=False)

    #
    # Process data.
    #
    # Pivot to wide curated indicators with consistent units.
    tb = curate_indicators(tb_meadow)

    # Harmonize country names. EIA aggregate regions (e.g. "Africa", "OPEC") become suffixed
    # entities like "Africa (EIA)" and stay alongside individual countries.
    tb = paths.regions.harmonize_names(tb=tb)

    # Attach indicator-level metadata before adding regions, so aggregates inherit it.
    tb = attach_indicator_metadata(tb)

    # Add OWID region aggregates. Only sum extensive indicators; intensive ones (per capita,
    # per GDP) stay country-level only.
    extensive_columns = [c for c in tb.columns if c not in {"country", "year"} and c not in INTENSIVE_INDICATORS]
    aggregations = {col: "sum" for col in extensive_columns}
    tb = paths.regions.add_aggregates(
        tb=tb,
        regions=REGIONS,
        aggregations=aggregations,
        min_num_values_per_year=1,
        ignore_overlaps_of_zeros=True,
        accepted_overlaps=KNOWN_OVERLAPS,
    )

    # Set an appropriate index and sort.
    tb = tb.format(keys=["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
