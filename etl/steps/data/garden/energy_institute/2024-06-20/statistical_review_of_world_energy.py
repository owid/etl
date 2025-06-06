"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

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
PRICE_REFERENCE_YEAR = 2023

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
    # Cobalt production.
    "cobalt_kt": "cobalt_production_kt",
    # Cobalt reserves.
    "cobaltres_kt": "cobalt_reserves_kt",
    # Graphite production.
    "graphite_kt": "graphite_production_kt",
    # Graphite reserves.
    "graphiteres_kt": "graphite_reserves_kt",
    # Lithium production.
    "lithium_kt": "lithium_production_kt",
    # Lithium reserves.
    "lithiumres_kt": "lithium_reserves_kt",
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
    "electbyfuel_ren_power": "renewables_electricity_generation_twh",
    # Renewable consumption (input-equivalent).
    # 'renewables_ej': 'renewables_consumption_equivalent_ej',
    # Renewable (excluding hydropower) consumption (input-equivalent).
    # 'ren_power_ej': 'renewables_consumption_equivalent_ej',
    # Renewable (excluding hydropower) electricity generation.
    # 'ren_power_twh': 'renewables_electricity_generation_twh',
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
    "co2_mtco2": "total_co2_emissions_mtco2",
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
    "natural_gas__us__henry_hub": "gas_price_us_henry_hub_current_dollars_per_million_btu",
    "natural_gas__zeebrugge": "gas_price_zeebrugge_current_dollars_per_million_btu",
    # Oil prices.
    f"oil_crude_prices__dollar_{PRICE_REFERENCE_YEAR}": f"oil_price_crude_{PRICE_REFERENCE_YEAR}_dollars_per_barrel",
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
    "Africa": {
        "additional_members": [
            # Additional African regions (e.g. 'Other Western Africa (EI)') seem to be included in
            # 'Other Africa (EI)', therefore we ignore them when creating aggregates.
            "Other Africa (EI)",
        ],
    },
    "Asia": {
        "additional_members": [
            # Adding 'Other Asia Pacific (EI)' may include areas of Oceania in Asia.
            # However, it seems that this region is usually significantly smaller than Asia.
            # So, we are possibly overestimating Asia, but not by a significant amount.
            "Other Asia Pacific (EI)",
            # Similarly, adding 'Other CIS (EI)' in Asia may include areas of Europe in Asia (e.g. Moldova).
            # However, since most countries in 'Other CIS (EI)' are Asian, adding it is more accurate than not adding it.
            "Other CIS (EI)",
            # Countries defined by EI in 'Middle East' are fully included in OWID's definition of Asia.
            "Other Middle East (EI)",
        ],
    },
    "Europe": {
        "additional_members": [
            "Other Europe (EI)",
        ],
    },
    "North America": {
        "additional_members": [
            "Other Caribbean (EI)",
            "Other North America (EI)",
        ],
    },
    "South America": {
        "additional_members": [
            "Other South America (EI)",
        ],
    },
    # Given that 'Other Asia and Pacific (EI)' is often similar or even larger than Oceania, we avoid including it in
    # Oceania (and include it in Asia, see comment above).
    # This means that we may be underestimating Oceania by a significant amount, but EI does not provide unambiguous
    # data to avoid this.
    "Oceania": {},
    # Income groups.
    "Low-income countries": {},
    "Lower-middle-income countries": {},
    "Upper-middle-income countries": {},
    "High-income countries": {},
}


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
        # Bangladesh is building its first nuclear power plant, to be commissioned in 2024.
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
        # Egypt is building its first nuclear power plant, to be commissioned in 2026.
        # https://en.wikipedia.org/wiki/El_Dabaa_Nuclear_Power_Plant
        "Egypt",
        "Equatorial Guinea",
        # Estonia has plans to built a nuclear power plant in 2024.
        # https://en.wikipedia.org/wiki/Nuclear_power_in_Estonia
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
        # https://world-nuclear.org/information-library/country-profiles/countries-t-z/turkey#:~:text=Turkey%20has%20had%20plans%20for,financed%20and%20built%20by%20Russia.
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
        error = "List of countries expected to have at empty or zero nuclear data has changed."
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

    ####################################################################################################################
    # Similarly to missing nuclear data, hydropower is missing from year 2000 onwards, at least for Saudi Arabia.
    # However, in the excel data file, those values are zero.
    # Fill those missing points with zeros.
    error = "Expected missing data for Arabia's hydropower from 2000 on. It may be fixed, so, remove this code."
    assert (
        tb.loc[
            (tb["country"] == "Saudi Arabia") & (tb["year"] >= 2000),
            ["hydro_consumption_equivalent_ej", "hydro_electricity_generation_twh"],
        ]
        .isnull()
        .all()
        .all()
    ), error
    tb.loc[
        (tb["country"] == "Saudi Arabia") & (tb["year"] >= 2000),
        ["hydro_consumption_equivalent_ej", "hydro_electricity_generation_twh"],
    ] = 0
    ####################################################################################################################

    # Create additional variables, like primary energy consumption in TWh (both direct and in input-equivalents).
    tb = create_additional_variables(tb=tb)

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

    # Add column for thermal equivalent efficiency factors.
    tb = tb.merge(tb_efficiency, how="left", on="year")

    # Remove "Other *" regions, since they mean different set of countries for different variables.
    # NOTE: They have to be removed *after* creating region aggregates, otherwise those regions would be underestimated.
    tb = tb[~tb["country"].str.startswith("Other ")].reset_index(drop=True)

    # Set an appropriate index to main table and sort conveniently.
    tb = tb.format(sort_columns=True)

    # Rename columns from the additional data file related to prices.
    tb_prices = tb_meadow_prices.rename(columns=COLUMNS_PRICES, errors="raise").copy()

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
    ds_garden = paths.create_dataset(tables=[tb, tb_prices, tb_prices_index], default_metadata=ds_meadow.metadata)
    ds_garden.save()
