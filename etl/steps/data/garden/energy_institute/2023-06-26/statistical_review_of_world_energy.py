"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Dataset, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# +
# TODO: Compare BP and EI definitions of regions in their methodology document.

# +
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
    "coal_reserves__total": "coal_reserves",
    "coal_reserves__anthracite_and_bituminous": "coal_anthracite_and_bituminous_reserves_mt",
    "coal_reserves__sub_bituminous_and_lignite": "coal_subbituminous_and_lignite_reserves_mt",
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
    "electbyfuel_other": "other_electricity_generation_twh",
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
    "primary_ej": "primary_energy_consumption_ej",
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
    "biodiesel_prod_kboed": "biodiesel_production_kboed",
    "biodiesel_prod_pj": "biodiesel_production_pj",
    # Biodiesel consumption.
    "biodiesel_cons_kboed": "biodiesel_consumption_kboed",
    "biodiesel_cons_pj": "biodiesel_consumption_pj",
    # Biofuels production.
    "biofuels_prod_kbd": "biofuels_production_kbd",
    "biofuels_prod_kboed": "biofuels_production_kboed",
    "biofuels_prod_pj": "biofuels_production_pj",
    # Biofuels consumption.
    "biofuels_cons_ej": "biofuels_consumption_ej",
    "biofuels_cons_kbd": "biofuels_consumption_kbd",
    "biofuels_cons_kboed": "biofuels_consumption_kboed",
    "biofuels_cons_pj": "biofuels_consumption_pj",
    # Oil production.
    "oilprod_kbd": "oil_production_kbd",
    "oilprod_mt": "oil_production_mt",
    # Oil consumption.
    "oilcons_ej": "oil_consumption_ej",
    "oilcons_kbd": "oil_consumption_kbd",
    "oilcons_mt": "oil_consumption_mt",
    # Oil electricity generation.
    "electbyfuel_oil": "oil_electricity_generation_twh",
    # Oil - Kerosene consumption.
    "kerosene_cons_kbd": "kerosene_consumption_kbd",
    # CO2 and methane emissions.
    "co2_mtco2": "total_co2_emissions_mtco2",
    # Other unused columns.
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
    "asian_marker_price": "coal_price_asian_marker",
    "china_qinhuangdao_spot_price": "coal_price_china_qinhuangdao_spot",
    "japan_coking_coal_import_cif_price": "coal_price_japan_coking_coal_import_cif",
    "japan_steam_coal_import_cif_price": "coal_price_japan_steam_coal_import_cif",
    "japan_steam_spot_cif_price": "coal_price_japan_steam_spot_cif",
    "lng__japan__cif": "gas_price_lng_japan_cif",
    "lng__japan_korea_marker__jkm": "gas_price_lng_japan_korea_marker",
    "natural_gas__average_german__import_price": "gas_price_average_german_import",
    "natural_gas__canada__alberta": "gas_price_canada_alberta",
    "natural_gas__netherlands_ttf__da_icis__heren_ttf_index": "gas_price_netherlands_ttf_index",
    "natural_gas__uk_nbp__icis_nbp_index": "gas_price_uk_nbp_index",
    "natural_gas__us__henry_hub": "gas_price_us_henry_hub",
    "newcastle_thermal_coal_fob": "coal_price_newcastle_thermal_coal_fob",
    "northwest_europe": "coal_price_northwest_europe",
    "oil_crude_prices__dollar_2022": "oil_price_crude_dollar_2022",
    "oil_crude_prices__dollar_money_of_the_day": "oil_price_crude_dollar_money_of_the_day",
    "oil_spot_crude_prices__brent": "oil_price_crude_spot_brent",
    "oil_spot_crude_prices__dubai": "oil_price_crude_spot_dubai",
    "oil_spot_crude_prices__nigerian_forcados": "oil_price_crude_spot_nigerian_forcados",
    "oil_spot_crude_prices__west_texas_intermediate": "oil_price_crude_spot_west_texas_intermediate",
    "us_central_appalachian_coal_spot_price_index": "coal_price_us_central_appalachian_spot_price_index",
}
# -

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
}


def add_region_aggregates(tb: Table, ds_regions: Dataset, ds_income_groups: Dataset) -> Table:
    # Add region aggregates.
    for region in REGIONS:
        members = geo.list_members_of_region(
            region=region,
            ds_regions=ds_regions,
            ds_income_groups=ds_income_groups,
            additional_members=REGIONS[region].get("additional_members"),
        )
        tb = geo.add_region_aggregates(
            df=tb,
            region=region,
            countries_in_region=members,
            countries_that_must_have_data=[],
            num_allowed_nans_per_year=None,
            frac_allowed_nans_per_year=0.9999,
        )
    return tb


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table and the fossil fuel prices table.
    ds_meadow: Dataset = paths.load_dependency("statistical_review_of_world_energy")
    tb_meadow = ds_meadow["statistical_review_of_world_energy"].reset_index()
    tb_meadow_prices = ds_meadow["statistical_review_of_world_energy_fossil_fuel_prices"].reset_index()

    # Load regions dataset.
    ds_regions: Dataset = paths.load_dependency("regions")

    # Load income groups dataset.
    ds_income_groups: Dataset = paths.load_dependency("income_groups")

    #
    # Process data.
    #
    # Select necessary columns from the data, and rename them conveniently.
    tb = tb_meadow[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Harmonize country names.
    tb: Table = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    # Add region aggregates.
    tb = add_region_aggregates(tb=tb, ds_regions=ds_regions, ds_income_groups=ds_income_groups)

    # Copy metadata from original table.
    tb = tb.copy_metadata(from_table=tb_meadow)

    # Rename columns from the additional data file related to prices.
    tb_prices = tb_meadow_prices.rename(columns=COLUMNS_PRICES, errors="raise").copy()

    # Set an appropriate index to each table and sort conveniently.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    tb_prices = tb_prices.set_index(["year"], verify_integrity=True).sort_index().sort_index(axis=1)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb, tb_prices], default_metadata=ds_meadow.metadata)
    ds_garden.save()
