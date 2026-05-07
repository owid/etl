"""Garden step for EIA International Energy data.

Pivots the long-format meadow table to a wide table of curated indicators (energy and electricity
in TWh, installed capacity in GW, CO2 in million tonnes, etc.), harmonizes country names, and
adds OWID region aggregates.

Combines two upstream meadow tables:

- ``international_energy`` (current EIA bulk file) — supplies all live, regularly-updated
  indicators (production, consumption, electricity, etc.).
- ``international_energy_archive`` (frozen 2022 export from EIA's old international tool) —
  supplies indicators that EIA has since retired upstream and that don't exist anywhere else,
  notably **natural gas reserves** and **oil reserves**. These columns are frozen at 2021 and
  carry that limitation as metadata.
"""

import owid.catalog.processing as pr
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
    # Coal — energy (TWh).
    ("Coal consumption", "terajoules", "coal_consumption", TJ_TO_TWH, "terawatt-hours", "TWh", "Coal consumption"),
    ("Coal production", "terajoules", "coal_production", TJ_TO_TWH, "terawatt-hours", "TWh", "Coal production"),
    ("Coal imports", "terajoules", "coal_imports", TJ_TO_TWH, "terawatt-hours", "TWh", "Coal imports"),
    ("Coal exports", "terajoules", "coal_exports", TJ_TO_TWH, "terawatt-hours", "TWh", "Coal exports"),
    # Coal — mass (Mt). Provided alongside the energy versions so downstream steps that need
    # a physical-mass series (e.g. the fossil-fuels explorer) can use them directly without
    # an approximate energy-density conversion.
    ("Coal consumption", "1000 metric tons", "coal_consumption_mt", 0.001, "million tonnes", "Mt", "Coal consumption"),
    ("Coal production", "1000 metric tons", "coal_production_mt", 0.001, "million tonnes", "Mt", "Coal production"),
    ("Coal imports", "1000 metric tons", "coal_imports_mt", 0.001, "million tonnes", "Mt", "Coal imports"),
    ("Coal exports", "1000 metric tons", "coal_exports_mt", 0.001, "million tonnes", "Mt", "Coal exports"),
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
    # Crude oil trade. NOTE: EIA's bulk file stops reporting these broadly after 2018 — full
    # country coverage is only available through 2018, with sparse data 2019–2020 and nothing
    # afterwards. They are still the best EIA series for crude trade.
    (
        "Crude oil including lease condensate imports",
        "thousand barrels per day",
        "crude_oil_imports",
        1.0,
        "thousand barrels per day",
        "kb/d",
        "Crude oil imports (including lease condensate)",
    ),
    (
        "Crude oil including lease condensate exports",
        "thousand barrels per day",
        "crude_oil_exports",
        1.0,
        "thousand barrels per day",
        "kb/d",
        "Crude oil exports (including lease condensate)",
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

# Indicators sourced from the frozen 2022 archive of EIA's old international tool — these don't
# exist in the current bulk file because EIA retired the upstream tables.
# Each tuple: (archive_topic, archive_indicator, archive_unit, output_column, factor, output_unit, output_short_unit, title).
# 1 trillion cubic feet = 0.0283168 trillion cubic metres (TCM).
TCF_TO_TCM = 0.0283168
ARCHIVE_INDICATORS: list[tuple[str, str, str, str, float, str, str, str]] = [
    (
        "natural_gas_reserves",
        "natural gas reserves",
        "tcf",
        "natural_gas_reserves",
        TCF_TO_TCM,
        "trillion cubic metres",
        "TCM",
        "Natural gas reserves",
    ),
    (
        "oil_reserves",
        "crude oil including lease condensate reserves",
        "billion b",
        "oil_reserves",
        1.0,
        "billion barrels",
        "Gbbl",
        "Oil reserves",
    ),
]

# Monthly indicators we keep from the monthly meadow table (date-indexed).
# Tuple shape matches INDICATORS but the time column is "date" instead of "year".
MONTHLY_INDICATORS: list[tuple[str, str, str, float, str, str, str]] = [
    (
        "Crude oil including lease condensate production",
        "thousand barrels per day",
        "crude_oil_production_monthly",
        1.0,
        "thousand barrels per day",
        "kb/d",
        "Crude oil production, monthly (including lease condensate)",
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
# NOTE: The year range must exactly match the years where both entities have data; extra years
# trigger an "overlaps not found" warning because the function compares the full year-set dict.
# Bump the upper bound (exclusive) when a new update extends the data past the current year.
KNOWN_OVERLAPS = [{year: {"Aruba", "Netherlands Antilles"} for year in range(1986, 2025)}]


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


def curate_monthly_indicators(tb_meadow_monthly: Table) -> Table:
    """Pivot the monthly meadow table into a wide table with the indicators we keep."""
    selected_pairs = {(var, unit) for var, unit, *_ in MONTHLY_INDICATORS}
    keep = [(v, u) in selected_pairs for v, u in zip(tb_meadow_monthly["variable"], tb_meadow_monthly["unit"])]
    tb = tb_meadow_monthly[keep][["country", "date", "variable", "unit", "value"]].copy()
    pair_to_output = {(var, unit): (name, factor) for var, unit, name, factor, *_ in MONTHLY_INDICATORS}
    tb["value"] = tb["value"] * [pair_to_output[(v, u)][1] for v, u in zip(tb["variable"], tb["unit"])]
    tb["indicator"] = [pair_to_output[(v, u)][0] for v, u in zip(tb["variable"], tb["unit"])]
    tb = tb.drop(columns=["variable", "unit"])
    tb = tb.pivot(index=["country", "date"], columns="indicator", values="value").reset_index()
    tb.columns.name = None
    return tb


def curate_archive_indicators(tb_archive: Table) -> Table:
    """Pivot the archive's long-format annual table to a wide table with the indicators we keep.

    Rows are selected by (topic, indicator, unit), then the value is multiplied by the per-row
    conversion factor and stored in the named output column.
    """
    selected = {(t, i, u) for t, i, u, *_ in ARCHIVE_INDICATORS}
    keep = [(t, i, u) in selected for t, i, u in zip(tb_archive["topic"], tb_archive["indicator"], tb_archive["unit"])]
    tb = tb_archive[keep][["topic", "country", "year", "indicator", "unit", "value"]].copy()

    # Apply per-row factor and rename to output column.
    triple_to_target = {(t, i, u): (name, factor) for t, i, u, name, factor, *_ in ARCHIVE_INDICATORS}
    factors = [triple_to_target[(t, i, u)][1] for t, i, u in zip(tb["topic"], tb["indicator"], tb["unit"])]
    tb["value"] = tb["value"] * factors
    tb["target"] = [triple_to_target[(t, i, u)][0] for t, i, u in zip(tb["topic"], tb["indicator"], tb["unit"])]
    tb = tb.drop(columns=["topic", "indicator", "unit"])

    # Pivot wide.
    tb = tb.pivot(index=["country", "year"], columns="target", values="value").reset_index()
    tb.columns.name = None
    return tb


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
    for _topic, _ind, _u, name, _factor, output_unit, output_short_unit, title in ARCHIVE_INDICATORS:
        if name not in tb.columns:
            continue
        tb[name].metadata.title = title
        tb[name].metadata.unit = output_unit
        tb[name].metadata.short_unit = output_short_unit
    for _var, _unit, name, _factor, output_unit, output_short_unit, title in MONTHLY_INDICATORS:
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
    tb_meadow_monthly = ds_meadow.read("international_energy_monthly", safe_types=False)

    ds_archive = paths.load_dataset("international_energy_archive")
    tb_archive = ds_archive.read("annual", safe_types=False)

    #
    # Process data.
    #
    # Pivot bulk-derived indicators to wide.
    tb = curate_indicators(tb_meadow)
    # Pivot archive-derived indicators to wide.
    tb_archive_wide = curate_archive_indicators(tb_archive)

    # Harmonize country names on both sides using the local mapping (covers the EIA-style
    # regions like "Africa (EIA)" and the country names used in both bulk and archive).
    tb = paths.regions.harmonize_names(tb=tb)
    tb_archive_wide = paths.regions.harmonize_names(
        tb=tb_archive_wide, warn_on_missing_countries=False, warn_on_unused_countries=False
    )

    # Merge bulk and archive on (country, year). Archive only contributes the reserves columns.
    tb = pr.merge(tb, tb_archive_wide, on=["country", "year"], how="outer", short_name=paths.short_name)

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

    # Build a separate monthly table (date-indexed) — the bulk file has monthly oil production
    # for ~250 countries through the present, useful for the fossil-fuels explorer's "Monthly
    # production" view. We harmonize country names but skip OWID region aggregation here:
    # add_aggregates assumes a year column, and EIA's own "World" row is already present in the
    # source data.
    tb_monthly = curate_monthly_indicators(tb_meadow_monthly)
    tb_monthly = paths.regions.harmonize_names(
        tb=tb_monthly, warn_on_missing_countries=False, warn_on_unused_countries=False
    )
    tb_monthly = tb_monthly[~tb_monthly["country"].astype(str).str.endswith(" (EIA)")].reset_index(drop=True)
    tb_monthly = attach_indicator_metadata(tb_monthly)
    tb_monthly = tb_monthly.format(keys=["country", "date"], short_name=f"{paths.short_name}_monthly")

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb, tb_monthly])
    ds_garden.save()
