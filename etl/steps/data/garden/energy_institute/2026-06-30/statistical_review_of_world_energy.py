"""Load a meadow dataset and create a garden dataset.

Since the 2025 release, the Statistical Review of World Energy reports energy as Total Energy Supply
(TES), using the Physical Energy Content method, instead of the old "substitution" primary energy
consumption. From the 2026 release, the Energy Institute no longer publishes the old substitution
indicator at all, so we adopt TES.

Under TES (physical energy content method):
- Fossil fuels (coal, oil, gas): TES equals consumption, given as the gross calorific value of the
  fuel (including energy lost as heat during conversion). This is unchanged from the substitution
  method.
- Non-combustible renewables (wind, solar PV, hydro, ocean, wave): TES is simply the gross amount of
  electricity generated (no inflation by a thermal efficiency). Compared to the old substitution
  method (which inflated generation by ~1/0.4), these values are roughly 60% lower.
- Non-fossil sources whose primary input is heat (nuclear, geothermal, concentrating solar, biomass):
  the heat input is estimated using assumed thermal efficiencies (33% for nuclear). Compared to the
  old substitution method, nuclear is roughly 20% higher (0.33 vs ~0.40 basis).

"""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Unit conversion for gas reserves, which the source reports in trillion cubic meters.
TRILLION_CUBIC_METERS_TO_CUBIC_METERS = 1e12

# Unit conversion factors.
# Exajoules to terawatt-hours.
EJ_TO_TWH = 1e6 / 3600
# Petajoules to terawatt-hours.
PJ_TO_TWH = 1e3 / 3600
# Million tonnes of oil equivalent to petajoules (from the "Approximate conversion factors" sheet in the additional excel data file).
MTOE_TO_PJ = 41.868
# Million tonnes of oil equivalent to terawatt-hours.
MTOE_TO_TWH = MTOE_TO_PJ * PJ_TO_TWH
# Barrels to cubic meters.
BARRELS_TO_CUBIC_METERS = 1 / 6.2898
# Thousand barrels per day to cubic meters per day
KBD_TO_CUBIC_METERS_PER_DAY = 1000 * BARRELS_TO_CUBIC_METERS
# Million British thermal units (of natural gas and liquefied natural gas) to megawatt-hours.
MILLION_BTU_TO_MWH = 0.2931
# Billion barrels to tonnes.
BBL_TO_TONNES = 0.1364 * 1e9
# Pounds (lb) to kg.
LB_TO_KG = 0.453593

# Reference year to use for table of price indexes.
PRICE_INDEX_REFERENCE_YEAR = 2019

# There is overlapping data for gas_reserves_tcm from USSR and Russia between 1991 and 1996.
# By looking at the original file, this overlap seems to be intentional, so we keep the overlapping data when creating
# aggregates. To justify this choice, note that: (1) The numbers for USSR are significantly smaller than for Russia in
# that period (so there is probably no double-counting with Russia), (2) When the data for USSR ends, the data for
# Azerbaijan, Kazakhstan, Turkmenistan, Uzbekistan and Other CIS start (so there is no double-counting with those
# countries either).
KNOWN_OVERLAPS = [
    {
        1991: {"Russia", "USSR"},
        1992: {"Russia", "USSR"},
        1993: {"Russia", "USSR"},
        1994: {"Russia", "USSR"},
        1995: {"Russia", "USSR"},
        1996: {"Russia", "USSR"},
    }
]

# Columns to use from the main data file, and how to rename them.
# NOTE: Since the 2026 release, the consolidated dataset uses the naming "<source>_tes_ej" for Total
# Energy Supply by fuel (replacing the old "<source>cons_ej" columns). For fossil fuels, TES equals
# consumption (unchanged). For non-fossil sources, these are the new (physical-content) TES values.
COLUMNS = {
    # Index columns.
    "country": "country",
    "year": "year",
    # Total energy supply (headline series).
    "tes_ej": "total_energy_supply_ej",
    # Coal production.
    "coalprod_ej": "coal_production_ej",
    "coalprod_mt": "coal_production_mt",
    # Coal consumption (Total Energy Supply from coal; equals consumption for fossil fuels).
    "coal_tes_ej": "coal_consumption_ej",
    # Coal electricity generation.
    "electbyfuel_coal": "coal_electricity_generation_twh",
    # Coal reserves.
    "coal_reserves__total": "coal_reserves_mt",
    "coal_reserves__anthracite_and_bituminous": "coal_reserves_anthracite_and_bituminous_mt",
    "coal_reserves__sub_bituminous_and_lignite": "coal_reserves_subbituminous_and_lignite_mt",
    # Gas production.
    "gasprod_bcfd": "gas_production_bcfd",
    "gasprod_bcm": "gas_production_bcm",
    "gasprod_ej": "gas_production_ej",
    # Gas consumption.
    "gascons_bcfd": "gas_consumption_bcfd",
    "gascons_bcm": "gas_consumption_bcm",
    "gas_tes_ej": "gas_consumption_ej",
    # Gas electricity generation.
    "electbyfuel_gas": "gas_electricity_generation_twh",
    # Gas reserves.
    "gas_reserves_tcm": "gas_reserves_tcm",
    # Minerals (production and reserves).
    "cobalt_production_kt": "cobalt_production_kt",
    "cobalt_reserves_kt": "cobalt_reserves_kt",
    "natural_graphite_production_kt": "graphite_production_kt",
    "natural_graphite_reserves_kt": "graphite_reserves_kt",
    "lithium_production_kt": "lithium_production_kt",
    "lithium_reserves_kt": "lithium_reserves_kt",
    # Electricity generation.
    "elect_twh": "electricity_generation_twh",
    # Nuclear consumption (Total Energy Supply, physical energy content: heat input at ~33% efficiency).
    "nuclear_tes_ej": "nuclear_consumption_ej",
    # Nuclear electricity generation.
    "nuclear_twh": "nuclear_electricity_generation_twh",
    # Hydropower consumption (Total Energy Supply, gross generation).
    "hydro_tes_ej": "hydro_consumption_ej",
    # Hydropower electricity generation.
    "hydro_twh": "hydro_electricity_generation_twh",
    # Other renewables (geothermal, biomass and others) consumption (Total Energy Supply).
    "biogeo_tes_ej": "other_renewables_consumption_ej",
    # Other renewables (geothermal, biomass and others) electricity generation.
    "biogeo_twh": "other_renewables_electricity_generation_twh",
    # Solar consumption (Total Energy Supply, gross generation).
    "solar_tes_ej": "solar_consumption_ej",
    # Solar electricity generation.
    "solar_twh": "solar_electricity_generation_twh",
    # Wind consumption (Total Energy Supply, gross generation).
    "wind_tes_ej": "wind_consumption_ej",
    # Wind electricity generation.
    "wind_twh": "wind_electricity_generation_twh",
    # Renewables consumption (excluding hydro, Total Energy Supply).
    # "renewables_tes_ej": "renewables_consumption_excl_hydro_ej",
    # Biodiesel production.
    "biodiesel_prod_pj": "biodiesel_production_pj",
    # Biodiesel consumption (Total Energy Supply).
    "biodiesel_tes_pj": "biodiesel_consumption_pj",
    # Biofuels production.
    "biofuels_prod_pj": "biofuels_production_pj",
    # Biofuels consumption (Total Energy Supply).
    "biofuels_tes_ej": "biofuels_consumption_ej",
    # Oil production.
    "oilprod_mt": "oil_production_mt",
    # Oil consumption (Total Energy Supply from oil; equals consumption for fossil fuels).
    "oil_tes_ej": "oil_consumption_ej",
    "oilcons_kbd": "oil_consumption_kbd",
    "oilcons_mt": "oil_consumption_mt",
    # Oil electricity generation.
    "electbyfuel_oil": "oil_electricity_generation_twh",
    # Oil reserves.
    "oil_reserves_bbl": "oil_reserves_bbl",
}

# Columns to use from the additional data file related to prices, and how to rename them.
COLUMNS_PRICES = {
    # Ammonia prices.
    "ammonia__far_east_asia": "ammonia_price_far_east_asia_current_dollars_per_tonne",
    "ammonia__middle_east": "ammonia_price_middle_east_current_dollars_per_tonne",
    "ammonia__northwest_europe": "ammonia_price_northwest_europe_current_dollars_per_tonne",
    "ammonia__us_gulf_coast": "ammonia_price_us_gulf_coast_current_dollars_per_tonne",
    # Coal prices.
    "coal__australia": "coal_price_australia_current_dollars_per_tonne",
    "coal__colombia": "coal_price_colombia_current_dollars_per_tonne",
    "coal__indonesia": "coal_price_indonesia_current_dollars_per_tonne",
    "coal__japan": "coal_price_japan_current_dollars_per_tonne",
    "coal__northwest_europe": "coal_price_northwest_europe_current_dollars_per_tonne",
    "coal__south_africa": "coal_price_south_africa_current_dollars_per_tonne",
    "coal__south_china": "coal_price_south_china_current_dollars_per_tonne",
    "coal__united_states": "coal_price_united_states_current_dollars_per_tonne",
    # NOTE: Hydrogen prices were removed by the Energy Institute in the 2026 release.
    # LNG prices.
    "lng__china__mainland": "lng_price_china_mainland_current_dollars_per_million_btu",
    "lng__japan": "lng_price_japan_current_dollars_per_million_btu",
    "lng__south_korea": "lng_price_south_korea_current_dollars_per_million_btu",
    # LNG marker prices (added by the Energy Institute in the 2026 release).
    "lng__japan_korea_marker__platts_jkm": "lng_price_japan_korea_marker_current_dollars_per_million_btu",
    "lng__north_west_european_marker__platts_nwe": "lng_price_northwest_europe_marker_current_dollars_per_million_btu",
    "lng__west_india_marker__platts_wim": "lng_price_west_india_marker_current_dollars_per_million_btu",
    # Natural gas prices.
    "natural_gas__netherlands_ttf": "gas_price_netherlands_ttf_current_dollars_per_million_btu",
    "natural_gas__uk_nbp": "gas_price_uk_nbp_current_dollars_per_million_btu",
    "natural_gas__us_henry_hub": "gas_price_us_henry_hub_current_dollars_per_million_btu",
    "natural_gas__zeebrugge": "gas_price_zeebrugge_current_dollars_per_million_btu",
    # Oil prices.
    # The constant-dollar oil crude price column is renamed later, once its reference year is known.
    "oil_crude_prices__dollar_money_of_the_day": "oil_price_crude_current_dollars_per_barrel",
    "oil_spot_crude_prices__brent": "oil_spot_crude_price_brent_current_dollars_per_barrel",
    "oil_spot_crude_prices__dubai": "oil_spot_crude_price_dubai_current_dollars_per_barrel",
    "oil_spot_crude_prices__nigerian_forcados": "oil_spot_crude_price_nigerian_forcados_current_dollars_per_barrel",
    "oil_spot_crude_prices__west_texas_intermediate": "oil_spot_crude_price_west_texas_intermediate_current_dollars_per_barrel",
    # Uranium prices.
    "uranium__canada": "uranium_price_canada_current_dollars_per_lb",
    # Old columns (not anymore existing in the current version of the Statistical Review).
    # Coal prices.
    # "asian_marker_price": "coal_price_asian_marker_current_dollars_per_tonne",
    # "china_qinhuangdao_spot_price": "coal_price_china_qinhuangdao_spot_current_dollars_per_tonne",
    # "japan_coking_coal_import_cif_price": "coal_price_japan_coking_coal_import_cif_current_dollars_per_tonne",
    # "japan_steam_coal_import_cif_price": "coal_price_japan_steam_coal_import_cif_current_dollars_per_tonne",
    # "japan_steam_spot_cif_price": "coal_price_japan_steam_spot_cif_current_dollars_per_tonne",
    # "us_central_appalachian_coal_spot_price_index": "coal_price_us_central_appalachian_spot_price_index_current_dollars_per_tonne",
    # "newcastle_thermal_coal_fob": "coal_price_newcastle_thermal_coal_fob_current_dollars_per_tonne",
    # "northwest_europe": "coal_price_northwest_europe_current_dollars_per_tonne",
    # Gas prices.
    # "lng__japan__cif": "gas_price_lng_japan_cif_current_dollars_per_million_btu",
    # "lng__japan_korea_marker__jkm": "gas_price_lng_japan_korea_marker_current_dollars_per_million_btu",
    # "natural_gas__average_german__import_price": "gas_price_average_german_import_current_dollars_per_million_btu",
    # "natural_gas__canada__alberta": "gas_price_canada_alberta_current_dollars_per_million_btu",
    # "natural_gas__netherlands_ttf__da_icis__heren_ttf_index": "gas_price_netherlands_ttf_index_current_dollars_per_million_btu",
    # "natural_gas__uk_nbp__icis_nbp_index": "gas_price_uk_nbp_index_current_dollars_per_million_btu",
    # "natural_gas__us__henry_hub": "gas_price_us_henry_hub_current_dollars_per_million_btu",
}

# Regions to use to create aggregates.
REGIONS = {
    ####################################################################################################################
    # NOTE: Given that the definition of Africa is the same for OWID and EI, and given some of the issues mentioned below, we will remove the aggregate for Africa, and simply copy the original one by EI. So the list of "additional_members" here is only kept for information purposes and sanity checks.
    "Africa": {
        "additional_members": [
            # Some indicators have Other Northern Africa and Other Southern Africa, while other indicators have Other Africa. But there is no indicator where both Other Northern or Southern Africa and Other Africa are informed at the same time (this is asserted in the code). So we can safely sum Other Northern Africa, Other Southern Africa, and Other Africa.
            "Other Northern Africa (EI)",
            "Other Southern Africa (EI)",
            "Other Africa (EI)",
            # There are also Other Eastern/Middle/Western Africa regions, but they are always empty or zero (this is asserted in code), so they can be ignored.
            "Other Eastern Africa (EI)",
            "Other Middle Africa (EI)",
            "Other Western Africa (EI)",
            # NOTE: I detected that, in the consolidated dataset, for biofuels consumption "Eastern Africa (EI)" coincides with "Other Africa (EI)", which is probably a mistake. Meanwhile, in the spreadsheet, there is only data for "Total Africa" (which is nonzero, despite no African country being informed).
            "Western Africa (EI)",
            "Middle Africa (EI)",
            "Eastern Africa (EI)",
        ],
    },
    ####################################################################################################################
    "Asia": {
        "additional_members": [
            # The region 'Other Asia Pacific (EI)' may include countries of both Oceania and Asia (according to OWID definitions). Unfortunately, the Statistical Review does not define "Oceania" explicitly in their "Definitions" sheet. However, it seems reasonable to expect that the main (and possibly only) country in "Other Asia Pacific" that belongs to OWID's Oceania would be Papua New Guinea. Other Oceanic countries like Samoa, Kiribati, or Vanuatu, are probably not included, or contributing minimally to the continent, for all indicators. We assume that Papua New Guinea is a small fraction of both Oceania, and Asia. Therefore, we include "Other Asia Pacific (EI)" under "Asia".
            # This means that we might be underestimating Oceania, and overestimating Asia, but not by a significant amount.
            # We correct for this issue in Asia. To do so, we remove the aggregate for Asia on any indicators where "Other Asia Pacific (EI)" exceeds a certain fraction.
            # Note that the same correction cannot be done for Oceania. If we did, we would unnecessarily lose Oceania in many indicators (because the contribution of Asian countries in "Other Asia Pacific (EI)" would be significant).
            "Other Asia Pacific (EI)",
            # According to the Statistical Review's "Definitions" sheet, CIS includes four countries that are assigned to Europe in OWID's definition, namely 'Belarus', 'Moldova', 'Russia', 'Ukraine'.
            # However, in the data, Ukraine is always included as part of Europe; I therefore understand that Ukraine is considered part of CIS only when referring to historical USSR data.
            # Data for Belarus and Russia are usually informed explicitly in the data (under CIS countries).
            # Hence, the only European country that could be included in "Other CIS (EI)" is Moldova (which is likely a small fraction). The rest of "Other CIS (EI)" are countries that are assigned to Asia in OWID's definitions.
            # Therefore, it's safe to assign "Other CIS (EI)" to the Asian aggregate.
            # Still, for safety, remove the aggregate for Europe and Asia on indicators where "Other CIS (EI)" is a significant fraction of the aggregate. In practice (at least as of the 2025 release), "Other CIS (EI)" is never a significant fraction of "Asia" and it is only a significant (>15%) fraction of "Europe" in the case of electricity from gas.
            "Other CIS (EI)",
            # Countries defined by EI in 'Middle East' are fully included in OWID's definition of Asia.
            "Other Middle East (EI)",
        ],
    },
    # All countries in EI's definition of Europe are included in OWID's definition of Europe (except Georgia, that OWID includes in Asia).
    "Europe": {
        "additional_members": [
            "Other Europe (EI)",
        ],
    },
    # NOTE: There is also "Other S. & Cent. America" (renamed "Other South and Central America (EI)"). This cannot be mapped to either North America or South America. We simply keep it as a separate entity. This means we may be underestimating South America and North America, but not by a significant amount. To correct for this issue, on indicators where "Other South and Central America (EI)" becomes significant compared to South America, we remove the aggregate for South America (and idem for North America).
    "South America": {
        "additional_members": [
            "Other South America (EI)",
        ],
    },
    # NOTE: See caveat about "Other South and Central America (EI)" explained above.
    "North America": {
        "additional_members": [
            "Other Caribbean (EI)",
            "Other North America (EI)",
            "Central America (EI)",
        ],
    },
    # Given that 'Other Asia and Pacific (EI)' is often similar or even larger than Oceania, we avoid including it in Oceania (and include it in Asia, see comment above).
    # This means that we may be underestimating Oceania by a significant amount, but EI does not provide unambiguous data to avoid this.
    "Oceania": {},
    # Income groups.
    "Low-income countries": {},
    "Lower-middle-income countries": {},
    "Upper-middle-income countries": {},
    "High-income countries": {},
}

# Sources whose total energy supply is their gross electricity generation. Under the physical energy
# content method no thermal efficiency is applied to them, unlike nuclear and other renewables (where
# the heat input is estimated at ~33% efficiency) or the fossil fuels (where the supply is the fuel
# burned, not the electricity produced).
SOURCES_WITH_SUPPLY_EQUAL_TO_GENERATION = ["hydro", "solar", "wind"]

# Sources that add up to the total energy supply (in exajoules).
TES_SOURCES_EJ = [
    "coal_consumption_ej",
    "oil_consumption_ej",
    "gas_consumption_ej",
    "nuclear_consumption_ej",
    "hydro_consumption_ej",
    "solar_consumption_ej",
    "wind_consumption_ej",
    "other_renewables_consumption_ej",
    "biofuels_consumption_ej",
]

# Regions that don't need to be included as part of other region aggregates (unlike, e.g. "Other Africa (EI)", which needs to be added to "Africa").
REGIONS_NOT_ASSIGNED_TO_OTHER_REGIONS = [
    "Africa (EI)",
    "Asia Pacific (EI)",
    "CIS (EI)",
    "Europe (EI)",
    "Middle East (EI)",
    "Middle East and Africa (EI)",
    "Non-OECD (EI)",
    "Non-OPEC (EI)",
    "North America (EI)",
    "OECD (EI)",
    "OPEC (EI)",
    "Other South and Central America (EI)",
    "Rest of World (EI)",
    "South and Central America (EI)",
]

# Provider regions dropped from the output (after being used as inputs to our region aggregates).
# They are either residual buckets of EI's own table layout ("Other Western Africa") or regional
# slices with no definition in our regions dataset. Crucially, the residual buckets have no fixed
# composition: "Other Europe" can mean a different set of countries for each indicator, so the same
# entity name would quietly mean different things on different charts.
# Deliberately kept in the output: the defined "(EI)" regions (they have a stable composition and are
# used by the by-region charts) and self-explanatory organizations (OECD, OPEC).
EXCLUDED_PROVIDER_REGIONS = [
    "Central America (EI)",
    "Eastern Africa (EI)",
    "Middle Africa (EI)",
    "Middle East and Africa (EI)",
    "Non-OECD (EI)",
    "Non-OPEC (EI)",
    "Other Africa (EI)",
    "Other Asia Pacific (EI)",
    "Other CIS (EI)",
    "Other Caribbean (EI)",
    "Other Eastern Africa (EI)",
    "Other Europe (EI)",
    "Other Middle Africa (EI)",
    "Other Middle East (EI)",
    "Other North America (EI)",
    "Other Northern Africa (EI)",
    "Other South America (EI)",
    "Other South and Central America (EI)",
    "Other Southern Africa (EI)",
    "Other Western Africa (EI)",
    "Rest of World (EI)",
    "Western Africa (EI)",
]


def create_additional_variables(tb: Table) -> Table:
    tb = tb.copy()

    for column in tb.columns:
        if column.endswith("_ej"):
            # Convert all variables given in exajoules into terawatt-hours.
            tb[column.replace("_ej", "_twh")] = tb[column] * EJ_TO_TWH
        if column.endswith("_pj"):
            # Convert all variables given in petajoules into terawatt-hours.
            tb[column.replace("_pj", "_twh")] = tb[column] * PJ_TO_TWH
        if column in ["oil_production_mt"]:
            # Oil consumption is given in exajoules, which is already converted to twh (previous lines).
            # Oil production, however, is given in million tonnes, which we convert now to terawatt-hours.
            tb[column.replace("_mt", "_twh")] = tb[column] * MTOE_TO_TWH
        if column in ["oil_consumption_kbd"]:
            # Convert oil consumption given in thousand barrels per day to cubic meters per day.
            tb[column.replace("_kbd", "_m3d")] = tb[column] * KBD_TO_CUBIC_METERS_PER_DAY
        if column in ["oil_reserves_bbl"]:
            # Convert oil reserves given in billions of barrels to tonnes.
            tb[column.replace("_bbl", "_t")] = tb[column] * BBL_TO_TONNES

    return tb


def convert_price_units(tb_prices: Table) -> Table:
    tb_prices = tb_prices.copy()

    for column in tb_prices.columns:
        if column.endswith("_per_barrel"):
            # Convert variables given in dollars per barrel to dollars per cubic meter.
            tb_prices[column.replace("_per_barrel", "_per_m3")] = tb_prices[column] / BARRELS_TO_CUBIC_METERS
            tb_prices = tb_prices.drop(columns=[column])
        elif column.endswith("_per_million_btu"):
            # Convert variables given in dollars per million BTU to dollars per megawatt-hour.
            tb_prices[column.replace("_per_million_btu", "_per_mwh")] = tb_prices[column] / MILLION_BTU_TO_MWH
            tb_prices = tb_prices.drop(columns=[column])
        elif column.endswith("_per_lb"):
            # Convert price of uranium from dollars per pound (lb) to dollars per kg.
            tb_prices[column.replace("_per_lb", "_per_kg")] = tb_prices[column] / LB_TO_KG

    return tb_prices


def prepare_prices_index_table(tb_prices: Table) -> Table:
    # Select all price columns except for (global) oil crude prices.
    tb_prices_index = tb_prices[
        [
            column
            for column in tb_prices.columns
            if column.startswith(("coal_price_", "gas_price_", "oil_spot_crude_price_"))
        ]
    ].copy()

    # Find all years for which different price columns have data, and ensure that the reference year is among them.
    years = set(tb_prices.reset_index()["year"])
    for column in tb_prices_index.columns:
        years = years & set(tb_prices_index[[column]].dropna().reset_index()["year"])
        # Normalize prices so that they were exactly 100 on the reference year.
        new_column = (
            column.replace("coal_price_", "coal_price_index_")
            .replace("gas_price_", "gas_price_index_")
            .replace("oil_spot_crude_price_", "oil_spot_crude_price_index_")
        )
        tb_prices_index[new_column] = (
            tb_prices_index[column] * 100 / tb_prices_index.loc[PRICE_INDEX_REFERENCE_YEAR][column]
        )
        tb_prices_index = tb_prices_index.drop(columns=[column])

        # Update metadata.
        tb_prices_index[
            new_column
        ].metadata.description_short = (
            f"Average price measured as an energy index where prices in {PRICE_INDEX_REFERENCE_YEAR} = 100."
        )

    # Sanity check.
    assert PRICE_INDEX_REFERENCE_YEAR in years, (
        f"The chosen reference year {PRICE_INDEX_REFERENCE_YEAR} does not have data for all variables; either change this year, or remove this assertion (and some prices will be dropped)."
    )

    # Remove empty rows and columns.
    tb_prices_index = tb_prices_index.dropna(axis=1, how="all").dropna(how="all")

    # Sanity check.
    assert tb_prices_index.loc[PRICE_INDEX_REFERENCE_YEAR].round(2).unique().tolist() == [100], (
        "Price index is not well constructed."
    )

    # Update table metadata.
    tb_prices_index.metadata.short_name = "statistical_review_of_world_energy_price_index"

    return tb_prices_index


def fill_missing_total_energy_supply(tb: Table) -> Table:
    """Fill missing total energy supply by source with zeros, where the total confirms they are zero.

    The Statistical Review omits coal, oil, gas and biofuels for country-years where they are zero,
    unlike hydro, solar, wind and other renewables, which it always reports explicitly. Its own total
    confirms the omissions: wherever the total is informed, it equals the sum of the reported sources,
    leaving no room for the missing ones.
    """
    # NOTE: A total of exactly zero satisfies the assertion below for free (e.g. USSR 1985-1991, where
    # the Statistical Review reports zeros across the board), so those rows are excluded.
    informed = tb["total_energy_supply_ej"].fillna(0) > 0
    total = tb.loc[informed, "total_energy_supply_ej"]
    error = (
        "Total energy supply is no longer the sum of its sources, so a missing source can no longer be "
        "assumed to be zero."
    )
    assert ((total - tb.loc[informed, TES_SOURCES_EJ].sum(axis=1)).abs() <= 0.01 * total).all(), error

    for source in TES_SOURCES_EJ:
        tb.loc[informed & tb[source].isna(), source] = 0

    return tb


def fill_missing_electricity_generation(tb: Table) -> Table:
    """Fill missing electricity generation with total energy supply, where they are the same quantity.

    For hydro, solar and wind, the total energy supply *is* the gross electricity generation (see
    SOURCES_WITH_SUPPLY_EQUAL_TO_GENERATION), so the two columns hold the same number. The Statistical
    Review nevertheless leaves gaps in the generation columns where it reports the supply, and those
    gaps can be filled exactly rather than inferred.
    """
    for source in SOURCES_WITH_SUPPLY_EQUAL_TO_GENERATION:
        supply, generation = f"{source}_consumption_twh", f"{source}_electricity_generation_twh"
        informed = tb[supply].notna() & tb[generation].notna()
        error = f"Total energy supply of {source} is no longer identical to its electricity generation."
        deviation = (tb.loc[informed, supply] - tb.loc[informed, generation]).abs()
        assert (deviation <= 1e-3 * tb.loc[informed, supply].abs()).all(), error
        tb[generation] = tb[generation].fillna(tb[supply])

    return tb


def fix_missing_nuclear_energy_data(tb: Table) -> Table:
    # List of countries in the data that have never had nuclear power in their grid, based on:
    # https://www.foronuclear.org/en/nuclear-power/nuclear-power-in-the-world/
    # As well as Wikipedia and other sources.
    countries_without_nuclear = [
        "Algeria",
        "Angola",
        "Australia",
        "Austria",
        "Azerbaijan",
        "Bahrain",
        # Bangladesh is building its first nuclear power plant, expected to become operational in December 2025.
        # https://en.wikipedia.org/wiki/Rooppur_Nuclear_Power_Plant
        "Bangladesh",
        "Bolivia",
        "Brunei",
        # 'Central America (EI)',
        "Chad",
        "Chile",
        "Colombia",
        "Congo",
        "Croatia",
        "Cuba",
        "Curacao",
        "Cyprus",
        "Democratic Republic of Congo",
        "Denmark",
        # 'Eastern Africa (EI)',
        "Ecuador",
        # Egypt is building its first nuclear power plant, to be commissioned in 2028.
        # https://en.wikipedia.org/wiki/El_Dabaa_Nuclear_Power_Plant
        "Egypt",
        "Equatorial Guinea",
        # Estonia has plans to build a nuclear power plant, which could start operating in 2035.
        # https://www.world-nuclear-news.org/articles/estonia-starts-planning-process-for-smr-plant
        "Estonia",
        "Gabon",
        "Guyana",
        # Hong Kong imports electricity from mainland China.
        # NOTE: Despite importing nuclear power from China, in the data, nuclear_consumption_ej is zero (or nan).
        # https://en.wikipedia.org/wiki/Nuclear_energy_in_Hong_Kong
        "Hong Kong",
        "Iceland",
        "Indonesia",
        "Iraq",
        "Ireland",
        "Israel",
        "Kuwait",
        "Latvia",
        "Libya",
        "Luxembourg",
        "Madagascar",
        "Malaysia",
        # 'Middle Africa (EI)',
        # 'Middle East (EI)',
        # 'Middle East and Africa (EI)',
        "Mongolia",
        "Morocco",
        "Mozambique",
        "Myanmar",
        "Netherlands Antilles",
        "New Caledonia",
        "New Zealand",
        "Nigeria",
        # 'Non-OPEC (EI)',
        "North Macedonia",
        "Norway",
        # 'OPEC (EI)',
        "Oman",
        # 'Other Africa (EI)',
        # 'Other Asia Pacific (EI)',
        # 'Other CIS (EI)',
        # 'Other Caribbean (EI)',
        # 'Other Eastern Africa (EI)',
        # 'Other Middle Africa (EI)',
        # 'Other Middle East (EI)',
        # 'Other North America (EI)',
        # 'Other Northern Africa (EI)',
        # 'Other S. & Cent. America (EI)',
        # 'Other South America (EI)',
        # 'Other Southern Africa (EI)',
        # 'Other Western Africa (EI)',
        "Papua New Guinea",
        "Peru",
        "Philippines",
        "Poland",
        "Portugal",
        "Qatar",
        # 'Rest of World (EI)',
        "Saudi Arabia",
        "Serbia",
        "Singapore",
        "South Sudan",
        "Sri Lanka",
        "Sudan",
        "Syria",
        "Thailand",
        "Trinidad and Tobago",
        "Tunisia",
        # Turkey's first nuclear power reactor is now expected to be connected to the grid in 2025.
        # https://world-nuclear.org/information-library/country-profiles/countries-t-z/turkey
        "Turkey",
        "Turkmenistan",
        "Uzbekistan",
        "Venezuela",
        "Vietnam",
        # 'Western Africa (EI)',
        "Yemen",
        "Zambia",
        "Zimbabwe",
    ]
    # Columns related to nuclear data.
    columns_nuclear = ["nuclear_consumption_ej", "nuclear_electricity_generation_twh"]

    for column in columns_nuclear:
        error = "List of countries expected to have empty or zero nuclear data has changed."
        assert tb[(tb["country"].isin(countries_without_nuclear)) & (tb[column].fillna(0) > 0)].empty, error
        # For all these countries, simply fill nans with zeros, as they use no nuclear energy.
        tb.loc[tb["country"].isin(countries_without_nuclear), column] = 0

        # Now consider countries that have nuclear power at least for one year.
        # Fix their missing data in a case by case scenario.
        error = "Data for countries with partial nuclear energy has changed."

        # Belarus nuclear was first connected to the grid in 2020.
        # https://en.wikipedia.org/wiki/Astravets_Nuclear_Power_Plant
        country = "Belarus"
        assert tb[(tb["country"] == country) & (tb["year"] < 2020) & (tb[column].fillna(0) > 0)].empty, error
        assert tb[(tb["country"] == country) & (tb["year"] > 2020) & (tb[column].isnull())].empty, error
        tb.loc[(tb["country"] == country) & (tb["year"] < 2020), column] = 0

        # Iran started producing electricity in 2010 (and the first informed point is 2011).
        # https://en.wikipedia.org/wiki/Nuclear_facilities_in_Iran
        country = "Iran"
        assert tb[(tb["country"] == country) & (tb["year"] < 2011) & (tb[column].fillna(0) > 0)].empty, error
        assert tb[(tb["country"] == country) & (tb["year"] > 2011) & (tb[column].isnull())].empty, error
        tb.loc[(tb["country"] == country) & (tb["year"] < 2011), column] = 0

        # Italy uses no nuclear power since 1990.
        # https://en.wikipedia.org/wiki/Nuclear_power_in_Italy
        country = "Italy"
        assert tb[(tb["country"] == country) & (tb["year"] > 1990) & (tb[column].fillna(0) > 0)].empty, error
        assert tb[(tb["country"] == country) & (tb["year"] < 1990) & (tb[column].isnull())].empty, error
        tb.loc[(tb["country"] == country) & (tb["year"] > 1990), column] = 0

        # Soviet Union successors (starting having data in 1985):
        # Kazakhstan stopped using nuclear power in 1999.
        # https://en.wikipedia.org/wiki/Nuclear_power_in_Kazakhstan
        country = "Kazakhstan"
        assert tb[(tb["country"] == country) & (tb["year"] > 1999) & (tb[column].fillna(0) > 0)].empty, error
        assert tb[
            (tb["country"] == country) & (tb["year"] < 1999) & (tb["year"] > 1985) & (tb[column].isnull())
        ].empty, error
        tb.loc[(tb["country"] == country) & (tb["year"] > 1999), column] = 0

        # Lithuania stopped using nuclear power in 2009.
        # https://en.wikipedia.org/wiki/Nuclear_power_in_Lithuania
        country = "Lithuania"
        assert tb[(tb["country"] == country) & (tb["year"] > 2009) & (tb[column].fillna(0) > 0)].empty, error
        assert tb[
            (tb["country"] == country) & (tb["year"] < 2009) & (tb["year"] > 1985) & (tb[column].isnull())
        ].empty, error
        tb.loc[(tb["country"] == country) & (tb["year"] > 2009), column] = 0

        # UAE has nuclear power since 2020.
        # https://en.wikipedia.org/wiki/Nuclear_power_in_the_United_Arab_Emirates
        country = "United Arab Emirates"
        assert tb[(tb["country"] == country) & (tb["year"] < 2020) & (tb[column].fillna(0) > 0)].empty
        assert tb[(tb["country"] == country) & (tb["year"] > 2020) & (tb[column].isnull())].empty
        tb.loc[(tb["country"] == country) & (tb["year"] < 2020), column] = 0

        # For USSR and Russia we have nuclear data. The USSR data ends in 1984, and Russia data starts in 1985
        # (they have nuclear power since 1954, the first in the world).
        # Simply check that there's data for all years before 1985 for USSR and after 1985 for successors.
        error = "Expected nuclear data for all years prior to 1985 for the USSR and for successors from 1985 onwards."
        assert tb[(tb["country"] == "USSR") & (tb["year"] < 1985) & (tb[column].isnull())].empty, error
        for country in ["Russia", "Ukraine", "Kazakhstan", "Lithuania"]:
            assert tb[(tb["country"] == country) & (tb["year"] > 1985) & (tb[column].isnull())].empty, error

    return tb


def fix_issues_with_other_regions(tb: Table) -> Table:
    tb = tb.copy()
    # Dictionary of "Other *" regions, and the OWID regions with which they may overlap.
    # For example, "Other South and Central America (EI)" could be assigned to either "South America" or "North America" (which, according to OWID region definitions, includes Central America).
    # This function will check how big the contribution of the "Other *" region is with respect to the overlapping OWID regions; if too big, the OWID region aggregate will be removed for that indicator.
    # We do this to avoid creating region aggregates that significantly underestimates the true value for the region.
    # To justify this correction, note that for some indicators (e.g. oil electricity generation), "Other South and Central America (EI)" is actually larger than "South America".
    # See further explanations above, where REGIONS is defined.
    ei_regions_and_overlapping_owid_regions = {
        "Other South and Central America (EI)": ["South America", "North America"],
        "Other CIS (EI)": ["Asia", "Europe"],
        # NOTE: As explained above (where REGIONS are defined), we don't include "Oceania" here because most of "Other Asia Pacific (EI)" are Asian countries; including "Oceania" here would imply unnecessarily removing that aggregate for many indicators.
        "Other Asia Pacific (EI)": ["Asia"],
    }
    # Divisor applied to each indicator's range of values in the continent to define the minimum relevant magnitude (range / 15) below which "Other *" values are ignored.
    fraction_of_range = 15
    # Percentage (of "Other *" with respect to its containing aggregate region) above which the aggregate region will be removed.
    max_percentage_deviation = 15
    # Remove aggregates in columns for which an overlapping "Other *" region has a significant contribution, compared to the aggregate.
    for other_region, owid_regions in ei_regions_and_overlapping_owid_regions.items():
        tb_other = tb[(tb["country"] == other_region)].fillna(0).reset_index(drop=True)
        for continent in owid_regions:
            tb_continent = tb[(tb["country"] == continent)].fillna(0).reset_index(drop=True)
            for column in tb.drop(columns=["country", "year"]).columns:
                remove_aggregate = False
                # Define the minimum magnitude of values that we care about (the indicator's range in the continent divided by fraction_of_range).
                min_range = (tb_continent[column].max() - tb_continent[column].min()) / fraction_of_range
                # If the "Other *" region has any value larger than the minimum range, consider removing the aggregate.
                mask = tb_other[column] > min_range
                if mask.any():
                    max_dev = (100 * tb_other[mask][column] / (tb_continent[mask][column] + 1e-6)).max()
                    if max_dev > max_percentage_deviation:
                        # If any of the values for the "Other *" region is larger than 15% of the value for the continent, remove the aggregate.
                        remove_aggregate = True

                if remove_aggregate:
                    # DEBUGGING: Uncomment to plot cases where aggregate was removed.
                    # print(f"Removing {continent} aggregate for {column}")
                    # px.line(pd.concat([tb_other, tb_continent]), x="year", y=column, color="country", markers=True,title="TO BE REMOVED").show()
                    # Remove this aggregate.
                    tb.loc[(tb["country"] == continent), column] = None
                else:
                    pass
                    # DEBUGGING: Uncomment to plot cases where the aggregates were kept.
                    # px.line(pd.concat([tb_other, tb_continent]), x="year", y=column, color="country", markers=True).show()

    return tb


def create_region_aggregates(tb: Table) -> Table:
    # Sanity checks around the "Other *" regions. These are values that cannot be assigned to individual countries, but should be included in region aggregates.
    # Check that the additional members mentioned in REGIONS (defined above) are as expected.
    other_regions_found = set(tb[tb["country"].str.contains("(EI)", regex=False)]["country"])
    other_regions_expected = set(
        sum([member for member in [REGIONS[region].get("additional_members") for region in REGIONS] if member], [])
    )
    error = "Mismatch between expected 'Other *' regions and those found in the data."
    assert other_regions_found - other_regions_expected == set(REGIONS_NOT_ASSIGNED_TO_OTHER_REGIONS), error
    assert other_regions_expected - other_regions_found == set(), error

    # Check that EI regions do not overlap with EI subregions. For example, "Other Africa (EI)" should not be given whenever "Other Northern Africa (EI)" or "Other Southern Africa (EI)" are also informed
    # This check is not fulfilled in the consolidated dataset, e.g. for biofuels consumption.
    # NOTE: This check is no longer needed, since it only involves Africa, which is an aggregate we will import directly from EI. But keep the code for now in case other similar cases arise.
    # ei_regions_and_subregions = {
    #     "Other Africa (EI)": ["Other Northern Africa (EI)", "Other Southern Africa (EI)"],
    # }
    # for ei_region, ei_subregions in ei_regions_and_subregions.items():
    #     for column in tb.drop(columns=["country", "year"]).columns:
    #         _tb_ei_region = tb[(tb["country"] == ei_region) & (tb[column].fillna(0) > 0)]
    #         _tb_ei_subregions = tb[
    #             (tb["country"].isin(ei_subregions))
    #             & (tb[column].fillna(0) > 0)
    #         ]
    #         error = f"Found overlapping data for {ei_region} and {ei_subregions} in {column}."
    #         assert not ((len(_tb_ei_region) > 0) and (len(_tb_ei_subregions) > 0))

    # Add region aggregates.
    tb = paths.regions.add_aggregates(
        tb,
        regions=REGIONS,
        min_num_values_per_year=1,
        ignore_overlaps_of_zeros=True,
        accepted_overlaps=KNOWN_OVERLAPS,
    )

    # Fix issues with "Other South and Central America", which cannot be assigned to either North or South America.
    tb = fix_issues_with_other_regions(tb=tb)

    # NOTE: "Other *" regions mean different set of countries for different variables.
    # We could remove them to avoid confusion. But it can also create confusion if aggregates do not add up to the sum of the expected countries. For some indicators, "Other *" regions carry a significant value.

    # As mentioned above, given that OWID and EI define Africa in the same way, and given the issues around biofuels, we'll simply copy EI's aggregate.
    tb_africa = tb[tb["country"] == "Africa (EI)"].reset_index(drop=True).assign(**{"country": "Africa"})
    tb = pr.concat([tb[tb["country"] != "Africa"], tb_africa], ignore_index=True)

    # There's an additional complication. Sometimes, the spreadsheet has no data for individual countries of a region, but it does have data for the "Total" of the region.
    # For example, biofuels production (PJ) has data for "Total Africa", "Total CIS", and "Total Middle East", but there's no way to know where those values come from. Given that we ignore those totals (otherwise we would be double-counting regions) this creates a mismatch between the EI continents and our aggregates. To solve this, simply visually check (at least for the current release) when does this happen, apart from Africa. If there's any other case, it could be solved by creating their aggregate separately, including those continent totals. Cases detected:
    def _check_subregion_is_small_compared_to_region(tb, subregion, region, indicator, percentage):
        _tb_subregion = tb[tb["country"] == subregion][["year", indicator]]
        _tb_region = tb[tb["country"] == region][["year", indicator]]
        check = _tb_subregion.merge(_tb_region, on=["year"], how="inner", suffixes=("_subregion", "_region")).dropna()
        error = f"Expected '{subregion}' to be non-empty (despite no individual country being informed). This entity is now empty for {indicator}. Consider removing this fix."
        assert (not check.empty) and (not check[check[f"{indicator}_subregion"] > 0].empty), error
        error = f"Expected '{subregion}' {indicator} to be a small fraction of the aggregate for {region}. This is no longer the case. Consider removing the aggregate for {region} for {indicator}."
        assert ((check[f"{indicator}_subregion"] / check[f"{indicator}_region"] * 100) < percentage).all(), error

    # * Middle East, for "Coal Production - mt". In this case, simply check that Middle East is so small compared to Asia (less than 0.2%), that we can ignore its contribution.
    _check_subregion_is_small_compared_to_region(
        tb, subregion="Middle East (EI)", region="Asia", indicator="coal_production_mt", percentage=0.5
    )
    # * "Grid Scale BESS Capacity". This is so far not used, so we'll ignore it.
    # * CIS, Middle East, and Africa, for all biofuels production and consumption indicators. We don't need to check Africa, since we are using the aggregate from EI directly. For CIS and Middle East, we simply check that they are relatively small (less than 5%) compared to the aggregates for Asia and Europe.
    for indicator in [
        # "biofuels_production_pj",
        "biofuels_consumption_ej",
        # "biofuels_production_twh",
        "biofuels_consumption_twh",
    ]:
        for subregion in ["Middle East (EI)", "CIS (EI)"]:
            _check_subregion_is_small_compared_to_region(
                tb, subregion=subregion, region="Asia", indicator=indicator, percentage=5
            )
            _check_subregion_is_small_compared_to_region(
                tb, subregion=subregion, region="Europe", indicator=indicator, percentage=5
            )
    # NOTE: I suppose the previous issue indicates that there can be hidden contributions in the totals of regions in the spreadsheet, even when the data for individual countries are specified. We could programmatically detect these cases, but it would not be trivial. Hopefully this issue happens only when no individual country of a region is informed.

    return tb


def fix_zeros_in_nonexisting_regions(tb: Table) -> Table:
    ussr_successors = set(paths.regions.get_region("USSR")["successors"])
    for column in tb.drop(columns=["country", "year"]).columns:
        if column in ["gas_reserves_tcm"]:
            # For gas reserves, the data already contains nans. Simply double check, and do nothing.
            ussr_last_year = 1996
            error = f"Expected USSR to be nan > {ussr_last_year} for column {column}."
            _mask = (tb["country"] == "USSR") & (tb["year"] > ussr_last_year)
            assert (tb[_mask][column].isnull()).all(), error

            # Russia has data from 1991, while all other successors have data from 1996 on.
            error = f"Expected other USSR successors (except Russia) to be nan <= {ussr_last_year} for column {column}."
            _mask = (tb["country"].isin(ussr_successors - set(["Russia"]))) & (tb["year"] <= ussr_last_year)
            assert (tb[_mask][column].isnull()).all(), error

            error = f"Expected Russia to be nan <= 1991 for column {column}."
            _mask = (tb["country"].isin(["Russia"])) & (tb["year"] < 1991)
            assert (tb[_mask][column].isnull()).all(), error

            continue
        # For all other columns, ensure there is no data on years where the countries did not exist.
        elif column in ["oil_reserves_bbl", "oil_reserves_t"]:
            ussr_last_year = 1990
        else:
            ussr_last_year = 1984

        error = f"Expected USSR to be zero > {ussr_last_year} for column {column}."
        _mask = (tb["country"] == "USSR") & (tb["year"] > ussr_last_year)
        assert (tb[_mask][column].fillna(0) == 0).all(), error
        tb.loc[_mask, column] = None

        error = f"Expected USSR successors to be zero <= {ussr_last_year} for column {column}."
        _mask = (tb["country"].isin(ussr_successors)) & (tb["year"] <= ussr_last_year)
        assert (tb[_mask][column].fillna(0) == 0).all()
        tb.loc[_mask, column] = None

        # Remove zeros from other nonexisting regions.
        _other_european = ["Croatia", "Slovenia", "North Macedonia"]
        _mask = (tb["country"].isin(_other_european)) & (tb["year"] < 1990)
        error = f"Expected {_other_european} to have only zeros < 1990."
        assert (tb[_mask][column].fillna(0) == 0).all(), error
        tb.loc[_mask, column] = None

        # Remove spurious zeros for Serbia.
        _mask = (tb["country"] == "Serbia") & (tb["year"] < 2007)
        error = f"Expected data for Serbia to be zero < 2007 for column {column}."
        assert (tb[_mask][column].fillna(0) == 0).all(), error
        tb.loc[_mask, column] = None

        # Remove spurious zeros for South Sudan.
        _mask = (tb["country"] == "South Sudan") & (tb["year"] < 2012)
        error = f"Expected data for South Sudan to be zero < 2012 for column {column}."
        assert (tb[_mask][column].fillna(0) == 0).all(), error
        tb.loc[_mask, column] = None

        # Remove spurious zeros for Yemen.
        _mask = (tb["country"] == "Yemen") & (tb["year"] < 1985)
        error = f"Expected data for Yemen to be zero < 1985 for column {column}."
        assert (tb[_mask][column].fillna(0) == 0).all(), error
        tb.loc[_mask, column] = None

    # Check that other historical regions don't need to be handled, as they are not in the data.
    _other_historical = ["Czechoslovakia", "Montenegro"]
    error = f"Unexpected data found for {_other_historical}."
    assert set(tb["country"]) & set(_other_historical) == set(), error

    return tb


def sanity_check_inputs(tb: Table) -> None:
    # Table should be unique by (country, year).
    assert not tb.duplicated(subset=["country", "year"]).any(), "Duplicate (country, year) rows in the input data."

    # Total energy supply and fossil fuel consumption should never be negative.
    for column in ["total_energy_supply_ej", "coal_consumption_ej", "oil_consumption_ej", "gas_consumption_ej"]:
        assert tb[column].min() >= 0, f"Negative values found in {column} (source error or unit mistake)."

    # Verify that the 2026 release uses the physical energy content method for Total Energy Supply.
    # This is what produces the expected shifts relative to the old substitution method
    # (renewables ~60% lower, nuclear ~20% higher, fossil fuels unchanged).
    world = tb[(tb["country"] == "World") & (tb["year"] == tb["year"].max())].iloc[0]
    # Non-combustible renewables (wind, solar, hydro) count gross electricity: TES (in TWh) == generation.
    for source in ["solar", "hydro", "wind"]:
        tes_twh = world[f"{source}_consumption_ej"] * EJ_TO_TWH
        generation_twh = world[f"{source}_electricity_generation_twh"]
        ratio = tes_twh / generation_twh
        assert 0.98 < ratio < 1.02, (
            f"Expected {source} Total Energy Supply to equal gross electricity generation (physical energy "
            f"content method), but the ratio is {ratio:.3f}. The methodology may have changed."
        )
    # Nuclear counts primary heat at ~33% efficiency: TES (in TWh) == generation / ~0.33 (i.e. a ratio of ~3).
    nuclear_ratio = world["nuclear_consumption_ej"] * EJ_TO_TWH / world["nuclear_electricity_generation_twh"]
    assert 2.8 < nuclear_ratio < 3.2, (
        f"Expected nuclear Total Energy Supply to be electricity generation divided by a thermal efficiency of "
        f"~33% (a ratio of ~3), but the ratio is {nuclear_ratio:.3f}. The methodology may have changed."
    )


def sanity_check_outputs(tb: Table) -> None:
    # No column should be entirely NaN.
    assert tb.columns[tb.isna().all()].empty, f"Output has fully-NaN columns: {list(tb.columns[tb.isna().all()])}"

    tb_world = tb[tb["country"] == "World"]
    latest_year = tb_world["year"].max()
    world = tb_world[tb_world["year"] == latest_year].iloc[0]

    # World total energy supply should be in a plausible range (~600 EJ in the mid-2020s).
    assert 500 < world["total_energy_supply_ej"] < 750, (
        f"World total energy supply for {latest_year} ({world['total_energy_supply_ej']:.1f} EJ) is out of the "
        f"expected range."
    )

    # World total energy supply should approximately reconcile with the sum of its sources.
    sources = [
        "coal_consumption_ej",
        "oil_consumption_ej",
        "gas_consumption_ej",
        "nuclear_consumption_ej",
        "hydro_consumption_ej",
        "solar_consumption_ej",
        "wind_consumption_ej",
        "other_renewables_consumption_ej",
    ]
    sources_sum = sum(world[source] for source in sources)
    deviation = 100 * abs(sources_sum - world["total_energy_supply_ej"]) / world["total_energy_supply_ej"]
    assert deviation < 5, (
        f"World total energy supply ({world['total_energy_supply_ej']:.1f} EJ) does not reconcile with the sum of "
        f"its sources ({sources_sum:.1f} EJ); deviation is {deviation:.1f}%."
    )

    # Region aggregates should have been created.
    expected_regions = {"Africa", "Asia", "Europe", "North America", "South America", "Oceania"}
    missing_regions = expected_regions - set(tb["country"])
    assert not missing_regions, f"Missing expected region aggregates: {missing_regions}"


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its tables.
    ds_meadow = paths.load_dataset("statistical_review_of_world_energy")
    tb_meadow = ds_meadow.read("statistical_review_of_world_energy")
    tb_meadow_prices = ds_meadow.read("statistical_review_of_world_energy_prices")

    #
    # Process data.
    #
    # Select necessary columns from the data, and rename them conveniently.
    tb = tb_meadow[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb)

    # Correct known upstream data errors (see the accompanying .corrections.yml).
    tb = paths.apply_corrections(tb)

    # Sanity-check the input data (before any transformation).
    sanity_check_inputs(tb=tb)

    # Fill spurious nans in nuclear energy data with zeros.
    tb = fix_missing_nuclear_energy_data(tb=tb)

    # Fill missing total energy supply by source with zeros, where the total confirms they are zero.
    tb = fill_missing_total_energy_supply(tb=tb)

    # Create additional variables (e.g. energy given in exajoules, also converted to terawatt-hours).
    tb = create_additional_variables(tb=tb)

    # Fill missing electricity generation with total energy supply, where they are the same quantity.
    tb = fill_missing_electricity_generation(tb=tb)

    # Create region aggregates and fix various related issues.
    tb = create_region_aggregates(tb=tb)

    # Remove spurious zeros in nonexisting regions (e.g. USSR after its dissolution).
    tb = fix_zeros_in_nonexisting_regions(tb=tb)

    # Sanity-check the output data.
    sanity_check_outputs(tb=tb)

    # Remove residual and undefined provider regions (inputs to our aggregates above, but with no
    # stable meaning for readers; see EXCLUDED_PROVIDER_REGIONS). The meadow table keeps them all.
    tb = tb[~tb["country"].isin(EXCLUDED_PROVIDER_REGIONS)].reset_index(drop=True)

    # Convert gas reserves from trillion cubic meters to cubic meters. Done here rather than in the
    # grapher step because it changes the values, and it is the unit every consumer wants: the
    # fossil-fuels step reports reserves in cubic meters, as does the chart. The tcm name is kept until
    # this point because the checks above are written against the source's own column names.
    tb = tb.rename(columns={"gas_reserves_tcm": "gas_reserves_m3"}, errors="raise")
    tb["gas_reserves_m3"] *= TRILLION_CUBIC_METERS_TO_CUBIC_METERS

    # Set an appropriate index to main table and sort conveniently.
    tb = tb.format(sort_columns=True)

    # Rename columns from the additional data file related to prices.
    tb_prices = tb_meadow_prices.rename(columns=COLUMNS_PRICES, errors="raise").copy()
    # Fetch the reference year of the price from the publication date of the dataset (assume it's the year prior to publication).
    price_reference_year = int(tb_meadow_prices["year"].m.origins[0].date_published.split("-")[0]) - 1
    tb_prices = tb_prices.rename(
        columns={f"oil_crude_prices__dollar_{price_reference_year}": "oil_price_crude_constant_dollars_per_barrel"},
        errors="raise",
    )

    # Convert units of price variables.
    tb_prices = convert_price_units(tb_prices=tb_prices)

    # Set an appropriate index to prices table and sort conveniently.
    tb_prices = tb_prices.format(keys=["year"], sort_columns=True)

    # Create table of index prices (similar to tb_prices, but normalized so that prices are 100 in a reference year).
    tb_prices_index = prepare_prices_index_table(tb_prices=tb_prices)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(
        tables=[tb, tb_prices, tb_prices_index],
        default_metadata=ds_meadow.metadata,
        yaml_params={"price_reference_year": price_reference_year},
    )
    ds_garden.save()
