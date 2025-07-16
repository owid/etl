"""Load a meadow dataset and create a garden dataset.

The Statistical Review of World Energy changed its methodology to calculate energy consumption in the 2025 release.

Previously, it reported primary energy consumption using what we call the substitution method, where non-fossil power generation (all sources except coal, gas, and oil) was expressed as input-equivalent energy.

With the substitution method:
- Fossil fuels: Primary energy consumption included the gross calorific value of fossil fuels (i.e., including energy lost as heat during conversion).
- Non-fossil sources (nuclear and renewables): Energy consumption figures were calculated by inflating electricity generation by a factor of roughly 1/0.4 (where the denominator represents the efficiency of a standard thermal power plant; more specifically, it's a factor that's assumed to go from 36% to 41%, depending on the year). For biomass generation, that factor is 32% for all years (as explained in their 2024 methodology).

That approach aimed to make non-fossil sources comparable with fossil fuels by assuming the former were "as inefficient" as fossil fuel power plants.

In the new methodology (from the 2025 release), energy is reported as Total Energy Supply (TES) using the Physical Energy Content method:
- Fossil fuels: TES is unchanged. It still includes the full gross calorific value, including energy wasted as heat.
- Non-combustible renewables (wind, solar PV, hydro, ocean, wave): TES is now simply the gross amount of electricity generated (assuming 100% efficiency; figures are not inflated).
- Non-fossil sources where the primary energy input is heat (nuclear, geothermal, concentrating solar): The heat input is estimated using assumed thermal efficiencies — 33% for nuclear and concentrating solar and biomass, and 10% for geothermal.

For now, we are ignoring TES and continuing to adapt consumption of non-fossil sources to match the old substitution method.
This is a temporary solution, as moving to the new methodology requires rewriting and adapting hundreds of charts and articles.

"""

import json

import owid.catalog.processing as pr
from owid.catalog import Dataset, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

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

# Reference year to use for table of prices.
PRICE_REFERENCE_YEAR = 2024

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
COLUMNS = {
    # Index columns.
    "country": "country",
    "year": "year",
    # Coal production.
    "coalprod_ej": "coal_production_ej",
    "coalprod_mt": "coal_production_mt",
    # Coal consumption.
    "coalcons_ej": "coal_consumption_ej",
    # Coal electricity generation.
    "electbyfuel_coal": "coal_electricity_generation_twh",
    # Coal reserves.
    "coal_reserves__total": "coal_reserves_mt",
    "coal_reserves__anthracite_and_bituminous": "coal_reserves_anthracite_and_bituminous_mt",
    "coal_reserves__sub_bituminous_and_lignite": "coal_reserves_subbituminous_and_lignite_mt",
    # 'coal_reserves__share_of_total': 'coal_reserves_share',
    # 'coal_reserves__r_p_ratio': 'coal_reserves_to_production_ratio',
    # Gas production.
    "gasprod_bcfd": "gas_production_bcfd",
    "gasprod_bcm": "gas_production_bcm",
    "gasprod_ej": "gas_production_ej",
    # Gas consumption.
    "gascons_bcfd": "gas_consumption_bcfd",
    "gascons_bcm": "gas_consumption_bcm",
    "gascons_ej": "gas_consumption_ej",
    # Gas electricity generation.
    "electbyfuel_gas": "gas_electricity_generation_twh",
    # Gas reserves.
    "gas_reserves_tcm": "gas_reserves_tcm",
    # NOTE: The following data for minerals does not follow the naming convention of the consolidated dataset, for convenience.
    # Cobalt production.
    "cobalt_production_kt": "cobalt_production_kt",
    # Cobalt reserves.
    "cobalt_reserves_kt": "cobalt_reserves_kt",
    # Graphite production.
    "natural_graphite_production_kt": "graphite_production_kt",
    # Graphite reserves.
    "natural_graphite_reserves_kt": "graphite_reserves_kt",
    # Lithium production.
    "lithium_production_kt": "lithium_production_kt",
    # Lithium reserves.
    "lithium_reserves_kt": "lithium_reserves_kt",
    # NOTE: There is data for other minerals that we could use here:
    # "rare_earth_metals_production_kt": "rare_earth_metals_production_kt",
    # "rare_earth_metals_reserves_kt": "rare_earth_metals_reserves_kt",
    # "copper_production_kt": "copper_production_kt",
    # "copper_reserves_kt": "copper_reserves_kt",
    # "manganese_production_kt": "manganese_production_kt",
    # "manganese_reserves_kt": "manganese_reserves_kt",
    # "nickel_production_kt": "nickel_production_kt",
    # "nickel_reserves_kt": "nickel_reserves_kt",
    # "zinc_production_kt": "zinc_production_kt",
    # "zinc_reserves_kt": "zinc_reserves_kt",
    # "platinum_group_metals_production_kt": "platinum_group_metals_production_kt",
    # "platinum_group_metals_reserves_kt": "platinum_group_metals_reserves_kt",
    # "bauxite_production_kt": "bauxite_production_kt",
    # "bauxite_reserves_kt": "bauxite_reserves_kt",
    # "aluminium_production_kt": "aluminium_production_kt",
    # "aluminium_capacity_kt": "aluminium_capacity_kt",
    # "tin_production_kt": "tin_production_kt",
    # "tin_reserves_kt": "tin_reserves_kt",
    # "vanadium_production_kt": "vanadium_production_kt",
    # "vanadium_reserves_kt": "vanadium_reserves_kt",
    # Electricity generation.
    "elect_twh": "electricity_generation_twh",
    # 'electbyfuel_total': 'electricity_generation_twh',
    # Other electricity generation.
    # "electbyfuel_other": "other_electricity_generation_twh",
    # Nuclear electricity consumption (input-equivalent).
    "nuclear_ej": "nuclear_consumption_equivalent_ej",
    # Nuclear electricity generation.
    "nuclear_twh": "nuclear_electricity_generation_twh",
    # 'nuclear_twh_net': 'nuclear_electricity_generation_net_twh',
    # 'electbyfuel_nuclear': 'nuclear_electricity_generation_twh',
    # Hydropower electricity consumption (input-equivalent).
    "hydro_ej": "hydro_consumption_equivalent_ej",
    # 'electbyfuel_hydro': 'electricity_by_fuel_hydro',
    # Hydropower electricity generation.
    "hydro_twh": "hydro_electricity_generation_twh",
    # 'hydro_twh_net': 'hydro_twh_net',
    # Other renewables (geothermal, biomass and others) consumption (input-equivalent).
    "biogeo_ej": "other_renewables_consumption_equivalent_ej",
    # Other renewables (geothermal, biomass and others) electricity generation.
    "biogeo_twh": "other_renewables_electricity_generation_twh",
    # 'biogeo_twh_net': 'biogeo_twh_net',
    # Primary energy consumption (non-fossil is given in input-equivalent).
    "primary_ej": "primary_energy_consumption_equivalent_ej",
    # Solar consumption (input-equivalent).
    "solar_ej": "solar_consumption_equivalent_ej",
    # Solar electricity generation.
    "solar_twh": "solar_electricity_generation_twh",
    # 'solar_twh_net': 'solar_electricity_generation_net_twh',
    # Wind consumption (input-equivalent).
    "wind_ej": "wind_consumption_equivalent_ej",
    # Wind electricity generation.
    "wind_twh": "wind_electricity_generation_twh",
    # 'wind_twh_net': 'wind_electricity_generation_net_twh',
    # Renewables electricity generation.
    "ren_power_inc_hydro_twh": "renewables_electricity_generation_twh",
    # Renewable consumption (input-equivalent).
    # 'renewables_ej': 'renewables_consumption_equivalent_ej',
    # Renewable (excluding hydropower) electricity generation.
    # "electbyfuel_ren_power": "",
    # 'ren_power_twh_net': 'renewables_electricity_generation_net_twh',
    # Biodiesel production.
    # "biodiesel_prod_kboed": "biodiesel_production_kboed",
    "biodiesel_prod_pj": "biodiesel_production_pj",
    # Biodiesel consumption.
    # "biodiesel_cons_kboed": "biodiesel_consumption_kboed",
    "biodiesel_cons_pj": "biodiesel_consumption_pj",
    # Biofuels production.
    # "biofuels_prod_kbd": "biofuels_production_kbd",
    # "biofuels_prod_kboed": "biofuels_production_kboed",
    "biofuels_prod_pj": "biofuels_production_pj",
    # Biofuels consumption.
    "biofuels_cons_ej": "biofuels_consumption_ej",
    # "biofuels_cons_kbd": "biofuels_consumption_kbd",
    # "biofuels_cons_kboed": "biofuels_consumption_kboed",
    # "biofuels_cons_pj": "biofuels_consumption_pj",
    # Oil production.
    # "oilprod_kbd": "oil_production_kbd",
    "oilprod_mt": "oil_production_mt",
    # Oil consumption.
    "oilcons_ej": "oil_consumption_ej",
    "oilcons_kbd": "oil_consumption_kbd",
    "oilcons_mt": "oil_consumption_mt",
    # Oil electricity generation.
    "electbyfuel_oil": "oil_electricity_generation_twh",
    # Oil reserves.
    "oil_reserves_bbl": "oil_reserves_bbl",
    # CO2 and methane emissions.
    # "co2_mtco2": "total_co2_emissions_mtco2",
    # Other unused columns.
    # "kerosene_cons_kbd": "kerosene_consumption_kbd",
    # 'oilprod_crudecond_kbd': 'oil_crude_oil_production_kbd',
    # 'oilprod_ngl_kbd': 'oil_production_ngl_kbd',
    # 'other_oil_cons_kbd': 'oil_other_consumption_kbd',
    # 'co2_combust_mtco2': 'total_co2_emissions_mtco2',
    # 'co2_combust_pc': 'total_co2_emissions_pc',
    # 'co2_combust_per_ej': 'total_co2_emissions_per_ej',
    # 'gasflared_mtco2': 'flaring_co2_emissions_mtco2',
    # 'methane_process_mtco2': 'methane_process_mtco2',
    # 'primary_eintensity': 'primary_eintensity',
    # 'primary_ej_pc': 'primary_ej_pc',
    # 'diesel_gasoil_cons_kbd': 'diesel_gasoil_consumption_kbd',
    # 'ethanol_cons_kboed': 'ethanol_consumption_kboed',
    # 'ethanol_cons_pj': 'ethanol_consumption_pj',
    # 'ethanol_prod_kboed': 'ethanol_production_kboed',
    # 'ethanol_prod_pj': 'ethanol_production_pj',
    # 'fuel_oil_cons_kbd': 'fuel_oil_consumption_kbd',
    # 'gasflared_bcm': 'gasflared_bcm',
    # 'gasoline_cons_kbd': 'gasoline_consumption_kbd',
    # 'light_dist_cons_kbd': 'light_dist_consumption_kbd',
    # 'liqcons_kbd': 'liqconsumption_kbd',
    # 'lpg_cons_kbd': 'lpg_consumption_kbd',
    # 'middle_dist_cons_kbd': 'middle_dist_consumption_kbd',
    # 'naphtha_cons_kbd': 'naphtha_consumption_kbd',
    # 'rareearths_kt': 'rareearths_kt',
    # 'rareearthsres_kt': 'rareearthsres_kt',
    # 'refcap_kbd': 'refcap_kbd',
    # 'refcaputil_pct': 'refcaputil_pct',
    # 'refthru_kbd': 'refthru_kbd',
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
    # Hydrogen prices.
    "hydrogen__far_east_asia": "hydrogen_price_far_east_asia_current_dollars_per_kg",
    "hydrogen__middle_east": "hydrogen_price_middle_east_current_dollars_per_kg",
    "hydrogen__northwest_europe": "hydrogen_price_northwest_europe_current_dollars_per_kg",
    "hydrogen__us_gulf_coast": "hydrogen_price_us_gulf_coast_current_dollars_per_kg",
    # LNG prices.
    "lng__china__mainland": "lng_price_china_mainland_current_dollars_per_million_btu",
    "lng__japan": "lng_price_japan_current_dollars_per_million_btu",
    "lng__south_korea": "lng_price_south_korea_current_dollars_per_million_btu",
    # Natural gas prices.
    "natural_gas__netherlands_ttf": "gas_price_netherlands_ttf_current_dollars_per_million_btu",
    "natural_gas__uk_nbp": "gas_price_uk_nbp_current_dollars_per_million_btu",
    "natural_gas__us_henry_hub": "gas_price_us_henry_hub_current_dollars_per_million_btu",
    "natural_gas__zeebrugge": "gas_price_zeebrugge_current_dollars_per_million_btu",
    # Oil prices.
    # Oil crude prices will be renamed afterwards, once the reference year of the price is known.
    # f"oil_crude_prices__dollar_{PRICE_REFERENCE_YEAR}": f"oil_price_crude_{PRICE_REFERENCE_YEAR}_dollars_per_barrel",
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
            # Convert variables given in dollars per million BTU to dollars per kilocalorie.
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
    assert (
        PRICE_INDEX_REFERENCE_YEAR in years
    ), f"The chosen reference year {PRICE_INDEX_REFERENCE_YEAR} does not have data for all variables; either change this year, or remove this assertion (and some prices will be dropped)."

    # Remove empty rows and columns.
    tb_prices_index = tb_prices_index.dropna(axis=1, how="all").dropna(how="all")

    # Sanity check.
    assert tb_prices_index.loc[PRICE_INDEX_REFERENCE_YEAR].round(2).unique().tolist() == [
        100
    ], "Price index is not well constructed."

    # Update table metadata.
    tb_prices_index.metadata.short_name = "statistical_review_of_world_energy_price_index"

    return tb_prices_index


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
    columns_nuclear = ["nuclear_consumption_equivalent_ej", "nuclear_electricity_generation_twh"]

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
    # Fraction of the range (between maximum and minimum) above which discrepancies between "Other *" regions and their containing aggregate regions will be considered for removal.
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
                # Define the "minimum range" of values that we care about (which is 15% of the maximum range of values for this indicator in the continent).
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


def create_region_aggregates(tb: Table, ds_regions: Dataset, ds_income_groups: Dataset) -> Table:
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
    tb = geo.add_regions_to_table(
        tb,
        regions=REGIONS,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
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


def create_primary_energy_in_input_equivalents(tb: Table):
    # NOTE: In the latest methodology, they have not included a thermal efficiency factor for the latest year (I suppose that this is because thermal efficiency factors are only used for the (legacy) primary energy consumption). Check that, indeed, this factor is missing for the latest year, and then fill it with the previously informed year.
    error = "Expected efficiency factor to be missing for the latest year."
    assert set(tb[tb["efficiency_factor"].isnull()]["year"]) == {tb["year"].max()}, error
    error = "Table was expected to be sorted chronologically (to forward-fill missing thermal efficiency factors)."
    assert set(tb.groupby(["country"])["year"].diff().fillna(1)) == {1}, error
    tb["efficiency_factor"] = tb["efficiency_factor"].ffill()

    # Create primary energy consumption in input-equivalents.
    # NOTE: This only needs to be done to non-fossil generation sources (which are "inflated" to mimic fossil inefficiencies). Fossil fuels consumption is by construction identical to primary energy consumption.
    for source in ["nuclear", "hydro", "other_renewables", "solar", "wind"]:
        if source == "other_renewables":
            # As explained in the 2024 methodology (and since the 2022 release), for biomass power, they assume a constant efficiency of 32% for biomass power to better reflect the actual efficiency of biomass power plants.
            # NOTE that geothermal and other renewables are also included here, but we only have access to the aggregated "other renewables".
            tb[f"{source}_consumption_equivalent_twh"] = tb[f"{source}_electricity_generation_twh"] / 0.32
        else:
            tb[f"{source}_consumption_equivalent_twh"] = (
                tb[f"{source}_electricity_generation_twh"] / tb["efficiency_factor"]
            )

        tb[f"{source}_consumption_equivalent_ej"] = (
            tb[f"{source}_consumption_equivalent_twh"] / EJ_TO_TWH
        ).copy_metadata(tb[f"{source}_consumption_equivalent_ej"])

    # To check that the previous conversion is accurate, create a temporary column for total input-equivalent primary energy, and compare it with the primary energy consumption given in the original data.
    check = tb.copy()
    check["primary_energy_consumption_equivalent_twh_calculated"] = (
        check["hydro_consumption_equivalent_twh"].fillna(0)
        + check["solar_consumption_equivalent_twh"].fillna(0)
        + check["wind_consumption_equivalent_twh"].fillna(0)
        + check["other_renewables_consumption_equivalent_twh"].fillna(0)
        + check["nuclear_consumption_equivalent_twh"].fillna(0)
        + check["biofuels_consumption_twh"].fillna(0)
        + check["coal_consumption_twh"].fillna(0)
        + check["oil_consumption_twh"].fillna(0)
        + check["gas_consumption_twh"].fillna(0)
    )
    # Check that the resulting calculated primary energy coincides (within ~5%) with the original one given in the statistical review.
    check["dev"] = (
        100
        * (
            check["primary_energy_consumption_equivalent_twh_calculated"].fillna(0)
            - check["primary_energy_consumption_equivalent_twh"].fillna(0)
        )
        / check["primary_energy_consumption_equivalent_twh"].fillna(0)
    )
    error = "Unexpected issue during the calculation of the primary energy consumption."
    assert abs(check["dev"]).max() < 5, error

    # DEBUGGING: Uncomment to compare the resulting calculated primary energy consumption curves for non-fossil sources, with the ones from the 2024 release.
    # from etl.data_helpers.misc import compare_tables
    # from owid.catalog import find
    # old = find("statistical_review_of_world_energy", version="2024-06-20").sort_values("table").head(1).load().reset_index()
    # columns = [f"{source}_consumption_equivalent_twh" for source in ["hydro", "nuclear", "wind", "solar", "other_renewables"]]
    # countries = sorted(set(old["country"]) & set(tb["country"]))
    # year_max = old["year"].max()
    # compare_tables(old[old["year"] <= year_max], tb[tb["year"] <= year_max], columns=columns, countries=countries, max_num_charts = 100)

    # Compare the primary consumption calculated here, with the one provided in the 2024 statistical review.
    # print(f"Mean percentual deviation between new (calculated) and old (from 2024 Statistical Review) primary energy consumption for:")
    # # Negative percentages mean that the new values are systematically lower than the 2024 ones.
    # for source in ["hydro", "nuclear", "solar", "wind", "other_renewables"]:
    #     cols = ["country", "year", f"{source}_consumption_equivalent_twh"]
    #     compared = old[cols].merge(tb[cols], on=["country", "year"], how="inner", suffixes=("_old", "_new"))
    #     compared["dev"] = (compared[f"{source}_consumption_equivalent_twh_new"] - compared[f"{source}_consumption_equivalent_twh_old"]) / compared[f"{source}_consumption_equivalent_twh_old"]
    #     print(f"- {source}: {compared['dev'].mean():.2%}")
    #     px.line(compared[compared["country"]=="World"].drop(columns=["dev"]).melt(id_vars=["country", "year"]), x="year", y="value", color="variable", markers=True).show()
    # The resulting deviations are:
    # - hydro: -6.02%
    # - nuclear: -2.10%
    # - solar: -5.71%
    # - wind: -6.00%
    # - other_renewables: 8.54%

    # Now forget about the 2025 data. Take the 2024 data alone, and compare the old primary energy consumption of each source with the one obtained by dividing electricity generation by efficiency. Do we get the same deviations?
    # print(f"Using data from the 2024 Statistical Review alone, we now calculate the mean percentual deviation between the calculated primary energy consumption and the one given in the data:")
    # for source in ["hydro", "nuclear", "solar", "wind", "other_renewables"]:
    #     t = old[["country", "year", f"{source}_electricity_generation_twh", f"{source}_consumption_equivalent_twh", "efficiency_factor"]].copy()
    #     if source == "other_renewables":
    #         t[f"{source}_consumption_equivalent_twh_calculated"] = t[f"{source}_electricity_generation_twh"] / 0.32
    #     else:
    #         t[f"{source}_consumption_equivalent_twh_calculated"] = t[f"{source}_electricity_generation_twh"] / t["efficiency_factor"]
    # t["dev"] = (t[f"{source}_consumption_equivalent_twh_calculated"] - t[f"{source}_consumption_equivalent_twh"]) / t[f"{source}_consumption_equivalent_twh"]
    #     print(f"- {source}: {t['dev'].mean():.2%}")
    #     px.line(t[t["country"]=="World"].drop(columns=["dev", "efficiency_factor", f"{source}_electricity_generation_twh"]).melt(id_vars=["country", "year"]), x="year", y="value", color="variable", markers=True).show()
    # The resulting deviations are:
    # - hydro: -6.05%
    # - nuclear: -2.09%
    # - solar: -6.04%
    # - wind: -6.04%
    # - other_renewables: 0.71%
    # So, except for other renewables, we get very similar deviations as with the 2025 data.
    # This means that the main issue is not related to changes in the 2025 methodology. Rather, what they used to call primary energy consumption for non-fossil generation is not exactly equal to gross generation / efficiency factor, as they describe in the methodology (or, alternatively, the efficiency factors quoted in their methodology are different to the ones used to do the conversion).
    # By looking at the methodology document, I can't figure out the reason for this discrepancy (by which the calculated primary energy consumption of hydro, solar and wind is ~6% lower than the one quoted in the data). But this discrepancy has been there for at least various years.

    return tb


def fix_zeros_in_nonexisting_regions(tb: Table, ds_regions: Dataset) -> Table:
    ussr_successors = set(
        ds_regions["regions"].loc[json.loads(ds_regions["regions"].loc["OWID_USS"]["successors"])]["name"]
    )
    for column in tb.drop(columns=["country", "year", "efficiency_factor"]).columns:
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


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its tables.
    ds_meadow = paths.load_dataset("statistical_review_of_world_energy")
    tb_meadow = ds_meadow.read("statistical_review_of_world_energy")
    tb_meadow_prices = ds_meadow.read("statistical_review_of_world_energy_prices")
    tb_efficiency = ds_meadow.read("statistical_review_of_world_energy_efficiency_factors")

    # Load regions dataset.
    ds_regions = paths.load_dataset("regions")

    # Load income groups dataset.
    ds_income_groups = paths.load_dataset("income_groups")

    #
    # Process data.
    #
    # Select necessary columns from the data, and rename them conveniently.
    tb = tb_meadow[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Harmonize country names.
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    # Fill spurious nans in nuclear energy data with zeros.
    tb = fix_missing_nuclear_energy_data(tb=tb)

    # Create additional variables, like primary energy consumption in TWh (both direct and in input-equivalents).
    tb = create_additional_variables(tb=tb)

    # Create region aggregates and fix various related issues.
    tb = create_region_aggregates(tb=tb, ds_regions=ds_regions, ds_income_groups=ds_income_groups)

    # Add column for thermal equivalent efficiency factors.
    tb = tb.merge(tb_efficiency, how="left", on="year")

    # Create primary energy consumption in input equivalents for non-fossil sources (for consistency with previous releases).
    tb = create_primary_energy_in_input_equivalents(tb=tb)

    # Remove spurious zeros in nonexisting regions (e.g. USSR after its dissolution).
    tb = fix_zeros_in_nonexisting_regions(tb=tb, ds_regions=ds_regions)

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
