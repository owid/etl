"""OWID Energy dataset: one wide table combining the energy mix, the electricity mix and the fossil fuels tables.

All three inputs follow the Total Energy Supply methodology of the 2026 Statistical Review. Regions (for ISO
codes), population and GDP (Maddison Project Database) are added as context columns.

Column names follow one grammar, `<source>_<domain>_<measure>`, with the unit last and no abbreviations:
`hydro_energy_twh`, `hydro_energy_share_pct`, `hydro_electricity_per_capita_kwh`, `coal_production_twh`.
The dictionaries below map every column of every input table to its name in the output; None means the
column is not published. A second table, `column_mapping`, records the name each column had in the
previous release (the `owid-energy-data.csv` file), so that users can migrate.
"""

import numpy as np
from owid.catalog import Origin, Table
from owid.catalog import processing as pr

from etl.data_helpers.geo import add_gdp_to_table
from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Columns that identify a row or give context, kept first in the output.
CONTEXT_COLUMNS = ["country", "year", "iso_code", "population", "gdp"]

# Energy mix (total energy supply): the domain `energy` is added to every source column.
ENERGY_MIX_COLUMNS = {
    "biofuels_annual_change_pct": "biofuels_energy_annual_change_pct",
    "biofuels_annual_change_twh": "biofuels_energy_annual_change_twh",
    "biofuels_per_capita_kwh": "biofuels_energy_per_capita_kwh",
    "biofuels_share_including_biomass_pct": None,  # World only.
    "biofuels_share_pct": "biofuels_energy_share_pct",
    "biofuels_twh": "biofuels_energy_twh",
    "coal_annual_change_pct": "coal_energy_annual_change_pct",
    "coal_annual_change_twh": "coal_energy_annual_change_twh",
    "coal_per_capita_kwh": "coal_energy_per_capita_kwh",
    "coal_share_including_biomass_pct": None,  # World only.
    "coal_share_pct": "coal_energy_share_pct",
    "coal_twh": "coal_energy_twh",
    "fossil_fuels_annual_change_pct": "fossil_fuels_energy_annual_change_pct",
    "fossil_fuels_annual_change_twh": "fossil_fuels_energy_annual_change_twh",
    "fossil_fuels_per_capita_kwh": "fossil_fuels_energy_per_capita_kwh",
    "fossil_fuels_share_pct": "fossil_fuels_energy_share_pct",
    "fossil_fuels_twh": "fossil_fuels_energy_twh",
    "gas_annual_change_pct": "gas_energy_annual_change_pct",
    "gas_annual_change_twh": "gas_energy_annual_change_twh",
    "gas_per_capita_kwh": "gas_energy_per_capita_kwh",
    "gas_share_including_biomass_pct": None,  # World only.
    "gas_share_pct": "gas_energy_share_pct",
    "gas_twh": "gas_energy_twh",
    "hydro_annual_change_pct": "hydro_energy_annual_change_pct",
    "hydro_annual_change_twh": "hydro_energy_annual_change_twh",
    "hydro_per_capita_kwh": "hydro_energy_per_capita_kwh",
    "hydro_share_including_biomass_pct": None,  # World only.
    "hydro_share_pct": "hydro_energy_share_pct",
    "hydro_twh": "hydro_energy_twh",
    "low_carbon_energy_annual_change_pct": "low_carbon_energy_annual_change_pct",
    "low_carbon_energy_annual_change_twh": "low_carbon_energy_annual_change_twh",
    "low_carbon_energy_per_capita_kwh": "low_carbon_energy_per_capita_kwh",
    "low_carbon_energy_share_pct": "low_carbon_energy_share_pct",
    "low_carbon_energy_twh": "low_carbon_energy_twh",
    "nuclear_annual_change_pct": "nuclear_energy_annual_change_pct",
    "nuclear_annual_change_twh": "nuclear_energy_annual_change_twh",
    "nuclear_per_capita_kwh": "nuclear_energy_per_capita_kwh",
    "nuclear_share_including_biomass_pct": None,  # World only.
    "nuclear_share_pct": "nuclear_energy_share_pct",
    "nuclear_twh": "nuclear_energy_twh",
    "oil_annual_change_pct": "oil_energy_annual_change_pct",
    "oil_annual_change_twh": "oil_energy_annual_change_twh",
    "oil_per_capita_kwh": "oil_energy_per_capita_kwh",
    "oil_share_including_biomass_pct": None,  # World only.
    "oil_share_pct": "oil_energy_share_pct",
    "oil_twh": "oil_energy_twh",
    "other_renewables_annual_change_pct": "other_renewables_energy_annual_change_pct",
    "other_renewables_annual_change_twh": "other_renewables_energy_annual_change_twh",
    "other_renewables_per_capita_kwh": "other_renewables_energy_per_capita_kwh",
    "other_renewables_share_including_biomass_pct": None,  # World only.
    "other_renewables_share_pct": "other_renewables_energy_share_pct",
    "other_renewables_twh": "other_renewables_energy_twh",
    "renewables_annual_change_pct": "renewables_energy_annual_change_pct",
    "renewables_annual_change_twh": "renewables_energy_annual_change_twh",
    "renewables_per_capita_kwh": "renewables_energy_per_capita_kwh",
    "renewables_share_pct": "renewables_energy_share_pct",
    "renewables_twh": "renewables_energy_twh",
    "solar_and_wind_annual_change_pct": "solar_and_wind_energy_annual_change_pct",
    "solar_and_wind_annual_change_twh": "solar_and_wind_energy_annual_change_twh",
    "solar_and_wind_per_capita_kwh": "solar_and_wind_energy_per_capita_kwh",
    "solar_and_wind_share_pct": "solar_and_wind_energy_share_pct",
    "solar_and_wind_twh": "solar_and_wind_energy_twh",
    "solar_annual_change_pct": "solar_energy_annual_change_pct",
    "solar_annual_change_twh": "solar_energy_annual_change_twh",
    "solar_per_capita_kwh": "solar_energy_per_capita_kwh",
    "solar_share_including_biomass_pct": None,  # World only.
    "solar_share_pct": "solar_energy_share_pct",
    "solar_twh": "solar_energy_twh",
    "total_energy_supply_annual_change_pct": "total_energy_supply_annual_change_pct",
    "total_energy_supply_annual_change_twh": "total_energy_supply_annual_change_twh",
    "total_energy_supply_per_capita_kwh": "total_energy_supply_per_capita_kwh",
    "total_energy_supply_per_gdp_kwh_per_dollar": "total_energy_supply_per_gdp_kwh_per_dollar",
    "total_energy_supply_twh": "total_energy_supply_twh",
    "traditional_biomass_share_including_biomass_pct": None,  # World only.
    "traditional_biomass_twh": None,  # World only.
    "wind_annual_change_pct": "wind_energy_annual_change_pct",
    "wind_annual_change_twh": "wind_energy_annual_change_twh",
    "wind_per_capita_kwh": "wind_energy_per_capita_kwh",
    "wind_share_including_biomass_pct": None,  # World only.
    "wind_share_pct": "wind_energy_share_pct",
    "wind_twh": "wind_energy_twh",
}

# Electricity mix (annual table): renamed from the older grammar of that table.
ELECTRICITY_MIX_COLUMNS = {
    "bioenergy_generation__twh": "bioenergy_electricity_twh",
    "bioenergy_share_of_electricity__pct": "bioenergy_electricity_share_pct",
    "bioenergy_stacked_generation__twh": None,  # Zero-filled copy for stacked charts.
    "co2_intensity__gco2_kwh": "electricity_carbon_intensity_gco2_kwh",
    "coal_generation__twh": "coal_electricity_twh",
    "coal_share_of_electricity__pct": "coal_electricity_share_pct",
    "fossil_generation__twh": "fossil_fuels_electricity_twh",
    "fossil_share_of_electricity__pct": "fossil_fuels_electricity_share_pct",
    "gas_generation__twh": "gas_electricity_twh",
    "gas_share_of_electricity__pct": "gas_electricity_share_pct",
    "hydro_generation__twh": "hydro_electricity_twh",
    "hydro_share_of_electricity__pct": "hydro_electricity_share_pct",
    "low_carbon_generation__twh": "low_carbon_electricity_twh",
    "low_carbon_share_of_electricity__pct": "low_carbon_electricity_share_pct",
    "net_imports_share_of_demand__pct": "electricity_net_imports_share_of_demand_pct",
    "nuclear_generation__twh": "nuclear_electricity_twh",
    "nuclear_share_of_electricity__pct": "nuclear_electricity_share_pct",
    "oil_generation__twh": "oil_electricity_twh",
    "oil_share_of_electricity__pct": "oil_electricity_share_pct",
    "other_renewables_excluding_bioenergy_generation__twh": "other_renewables_excluding_bioenergy_electricity_twh",
    "other_renewables_excluding_bioenergy_share_of_electricity__pct": "other_renewables_excluding_bioenergy_electricity_share_pct",
    "other_renewables_generation__twh": None,  # Includes bioenergy in the years where the source does not separate them.
    "other_renewables_including_bioenergy_generation__twh": "other_renewables_including_bioenergy_electricity_twh",
    "other_renewables_including_bioenergy_share_of_electricity__pct": "other_renewables_including_bioenergy_electricity_share_pct",
    "per_capita_bioenergy_generation__kwh": "bioenergy_electricity_per_capita_kwh",
    "per_capita_bioenergy_stacked_generation__kwh": None,  # Zero-filled copy for stacked charts.
    "per_capita_coal_generation__kwh": "coal_electricity_per_capita_kwh",
    "per_capita_fossil_generation__kwh": "fossil_fuels_electricity_per_capita_kwh",
    "per_capita_gas_generation__kwh": "gas_electricity_per_capita_kwh",
    "per_capita_hydro_generation__kwh": "hydro_electricity_per_capita_kwh",
    "per_capita_low_carbon_generation__kwh": "low_carbon_electricity_per_capita_kwh",
    "per_capita_nuclear_generation__kwh": "nuclear_electricity_per_capita_kwh",
    "per_capita_oil_generation__kwh": "oil_electricity_per_capita_kwh",
    "per_capita_other_renewables_excluding_bioenergy_generation__kwh": "other_renewables_excluding_bioenergy_electricity_per_capita_kwh",
    "per_capita_other_renewables_generation__kwh": None,  # Includes bioenergy in the years where the source does not separate them.
    "per_capita_other_renewables_including_bioenergy_generation__kwh": "other_renewables_including_bioenergy_electricity_per_capita_kwh",
    "per_capita_renewable_generation__kwh": "renewables_electricity_per_capita_kwh",
    "per_capita_solar_and_wind_generation__kwh": "solar_and_wind_electricity_per_capita_kwh",
    "per_capita_solar_generation__kwh": "solar_electricity_per_capita_kwh",
    "per_capita_total_demand__kwh": "electricity_demand_per_capita_kwh",
    "per_capita_total_generation__kwh": "electricity_generation_per_capita_kwh",
    "per_capita_wind_generation__kwh": "wind_electricity_per_capita_kwh",
    "population": None,  # Added again below, for every row.
    "renewable_generation__twh": "renewables_electricity_twh",
    "renewable_share_of_electricity__pct": "renewables_electricity_share_pct",
    "solar_and_wind_generation__twh": "solar_and_wind_electricity_twh",
    "solar_and_wind_share_of_electricity__pct": "solar_and_wind_electricity_share_pct",
    "solar_generation__twh": "solar_electricity_twh",
    "solar_share_of_electricity__pct": "solar_electricity_share_pct",
    "total_demand__twh": "electricity_demand_twh",
    "total_electricity_share_of_primary_energy__pct": "electricity_share_of_total_energy_supply_pct",
    "total_emissions__mtco2": "electricity_emissions_mtco2",
    "total_generation__twh": "electricity_generation_twh",
    "total_net_imports__twh": "electricity_net_imports_twh",
    "wind_generation__twh": "wind_electricity_twh",
    "wind_share_of_electricity__pct": "wind_electricity_share_pct",
}

# Fossil fuels: production, trade, reserves, and consumption in physical units.
# Consumption in energy units is not published: for countries it is identical to the energy mix columns
# (`coal_energy_twh`, ...), and for the World before 1965 the two tables differ.
FOSSIL_FUELS_COLUMNS = {
    "coal_consumption_per_capita_kwh": None,  # Same as the energy mix column for countries; differs for the World before 1965.
    "coal_consumption_per_capita_tonnes": "coal_consumption_per_capita_tonnes",
    "coal_consumption_tonnes": "coal_consumption_tonnes",
    "coal_consumption_twh": None,  # Same as the energy mix column for countries; differs for the World before 1965.
    "coal_exports_per_capita_tonnes": "coal_exports_per_capita_tonnes",
    "coal_exports_tonnes": "coal_exports_tonnes",
    "coal_imports_per_capita_tonnes": "coal_imports_per_capita_tonnes",
    "coal_imports_tonnes": "coal_imports_tonnes",
    "coal_net_imports_per_capita_tonnes": "coal_net_imports_per_capita_tonnes",
    "coal_net_imports_tonnes": "coal_net_imports_tonnes",
    "coal_production_annual_change_pct": "coal_production_annual_change_pct",
    "coal_production_annual_change_twh": "coal_production_annual_change_twh",
    "coal_production_per_capita_kwh": "coal_production_per_capita_kwh",
    "coal_production_per_capita_tonnes": "coal_production_per_capita_tonnes",
    "coal_production_tonnes": "coal_production_tonnes",
    "coal_production_twh": "coal_production_twh",
    "coal_reserves_per_capita_tonnes": "coal_reserves_per_capita_tonnes",
    "coal_reserves_to_production_ratio": None,  # World only.
    "coal_reserves_tonnes": "coal_reserves_tonnes",
    "gas_consumption_m3": "gas_consumption_m3",
    "gas_consumption_per_capita_kwh": None,  # Same as the energy mix column for countries; differs for the World before 1965.
    "gas_consumption_per_capita_m3": "gas_consumption_per_capita_m3",
    "gas_consumption_twh": None,  # Same as the energy mix column for countries; differs for the World before 1965.
    "gas_exports_m3": "gas_exports_m3",
    "gas_exports_per_capita_m3": "gas_exports_per_capita_m3",
    "gas_imports_m3": "gas_imports_m3",
    "gas_imports_per_capita_m3": "gas_imports_per_capita_m3",
    "gas_net_imports_m3": "gas_net_imports_m3",
    "gas_net_imports_per_capita_m3": "gas_net_imports_per_capita_m3",
    "gas_production_annual_change_pct": "gas_production_annual_change_pct",
    "gas_production_annual_change_twh": "gas_production_annual_change_twh",
    "gas_production_m3": "gas_production_m3",
    "gas_production_per_capita_kwh": "gas_production_per_capita_kwh",
    "gas_production_per_capita_m3": "gas_production_per_capita_m3",
    "gas_production_twh": "gas_production_twh",
    "gas_reserves_m3": "gas_reserves_m3",
    "gas_reserves_per_capita_m3": "gas_reserves_per_capita_m3",
    "gas_reserves_to_production_ratio": None,  # World only.
    "oil_consumption_m3": "oil_consumption_m3",
    "oil_consumption_per_capita_kwh": None,  # Same as the energy mix column for countries; differs for the World before 1965.
    "oil_consumption_per_capita_m3": "oil_consumption_per_capita_m3",
    "oil_consumption_twh": None,  # Same as the energy mix column for countries; differs for the World before 1965.
    "oil_exports_m3": "oil_exports_m3",
    "oil_exports_per_capita_m3": "oil_exports_per_capita_m3",
    "oil_imports_m3": "oil_imports_m3",
    "oil_imports_per_capita_m3": "oil_imports_per_capita_m3",
    "oil_net_imports_m3": "oil_net_imports_m3",
    "oil_net_imports_per_capita_m3": "oil_net_imports_per_capita_m3",
    "oil_production_annual_change_pct": "oil_production_annual_change_pct",
    "oil_production_annual_change_twh": "oil_production_annual_change_twh",
    "oil_production_m3": "oil_production_m3",
    "oil_production_per_capita_kwh": "oil_production_per_capita_kwh",
    "oil_production_per_capita_m3": "oil_production_per_capita_m3",
    "oil_production_twh": "oil_production_twh",
    "oil_reserves_m3": "oil_reserves_m3",
    "oil_reserves_per_capita_m3": "oil_reserves_per_capita_m3",
    "oil_reserves_to_production_ratio": None,  # World only.
    "total_consumption_per_capita_kwh": None,  # Same as the energy mix column for countries; differs for the World before 1965.
    "total_consumption_twh": None,  # Same as the energy mix column for countries; differs for the World before 1965.
    "total_production_per_capita_kwh": "fossil_fuels_production_per_capita_kwh",
    "total_production_twh": "fossil_fuels_production_twh",
}

# Names in the previous release of the dataset (owid-energy-data.csv), mapped to the new names.
PREVIOUS_COLUMNS = {
    "country": "country",
    "year": "year",
    "iso_code": "iso_code",
    "population": "population",
    "gdp": "gdp",
    "biofuel_cons_change_pct": "biofuels_energy_annual_change_pct",
    "biofuel_cons_change_twh": "biofuels_energy_annual_change_twh",
    "biofuel_cons_per_capita": "biofuels_energy_per_capita_kwh",
    "biofuel_consumption": "biofuels_energy_twh",
    "biofuel_elec_per_capita": "bioenergy_electricity_per_capita_kwh",
    "biofuel_electricity": "bioenergy_electricity_twh",
    "biofuel_share_elec": "bioenergy_electricity_share_pct",
    "biofuel_share_energy": "biofuels_energy_share_pct",
    "carbon_intensity_elec": "electricity_carbon_intensity_gco2_kwh",
    "coal_cons_change_pct": "coal_energy_annual_change_pct",
    "coal_cons_change_twh": "coal_energy_annual_change_twh",
    "coal_cons_per_capita": "coal_energy_per_capita_kwh",
    "coal_consumption": "coal_energy_twh",
    "coal_elec_per_capita": "coal_electricity_per_capita_kwh",
    "coal_electricity": "coal_electricity_twh",
    "coal_prod_change_pct": "coal_production_annual_change_pct",
    "coal_prod_change_twh": "coal_production_annual_change_twh",
    "coal_prod_per_capita": "coal_production_per_capita_kwh",
    "coal_production": "coal_production_twh",
    "coal_share_elec": "coal_electricity_share_pct",
    "coal_share_energy": "coal_energy_share_pct",
    "electricity_demand": "electricity_demand_twh",
    "electricity_demand_per_capita": "electricity_demand_per_capita_kwh",
    "electricity_generation": "electricity_generation_twh",
    "electricity_share_energy": "electricity_share_of_total_energy_supply_pct",
    "energy_cons_change_pct": "total_energy_supply_annual_change_pct",
    "energy_cons_change_twh": "total_energy_supply_annual_change_twh",
    "energy_per_capita": "total_energy_supply_per_capita_kwh",
    "energy_per_gdp": "total_energy_supply_per_gdp_kwh_per_dollar",
    "fossil_cons_change_pct": "fossil_fuels_energy_annual_change_pct",
    "fossil_cons_change_twh": "fossil_fuels_energy_annual_change_twh",
    "fossil_elec_per_capita": "fossil_fuels_electricity_per_capita_kwh",
    "fossil_electricity": "fossil_fuels_electricity_twh",
    "fossil_energy_per_capita": "fossil_fuels_energy_per_capita_kwh",
    "fossil_fuel_consumption": "fossil_fuels_energy_twh",
    "fossil_share_elec": "fossil_fuels_electricity_share_pct",
    "fossil_share_energy": "fossil_fuels_energy_share_pct",
    "gas_cons_change_pct": "gas_energy_annual_change_pct",
    "gas_cons_change_twh": "gas_energy_annual_change_twh",
    "gas_consumption": "gas_energy_twh",
    "gas_elec_per_capita": "gas_electricity_per_capita_kwh",
    "gas_electricity": "gas_electricity_twh",
    "gas_energy_per_capita": "gas_energy_per_capita_kwh",
    "gas_prod_change_pct": "gas_production_annual_change_pct",
    "gas_prod_change_twh": "gas_production_annual_change_twh",
    "gas_prod_per_capita": "gas_production_per_capita_kwh",
    "gas_production": "gas_production_twh",
    "gas_share_elec": "gas_electricity_share_pct",
    "gas_share_energy": "gas_energy_share_pct",
    "greenhouse_gas_emissions": "electricity_emissions_mtco2",
    "hydro_cons_change_pct": "hydro_energy_annual_change_pct",
    "hydro_cons_change_twh": "hydro_energy_annual_change_twh",
    "hydro_consumption": "hydro_energy_twh",
    "hydro_elec_per_capita": "hydro_electricity_per_capita_kwh",
    "hydro_electricity": "hydro_electricity_twh",
    "hydro_energy_per_capita": "hydro_energy_per_capita_kwh",
    "hydro_share_elec": "hydro_electricity_share_pct",
    "hydro_share_energy": "hydro_energy_share_pct",
    "low_carbon_cons_change_pct": "low_carbon_energy_annual_change_pct",
    "low_carbon_cons_change_twh": "low_carbon_energy_annual_change_twh",
    "low_carbon_consumption": "low_carbon_energy_twh",
    "low_carbon_elec_per_capita": "low_carbon_electricity_per_capita_kwh",
    "low_carbon_electricity": "low_carbon_electricity_twh",
    "low_carbon_energy_per_capita": "low_carbon_energy_per_capita_kwh",
    "low_carbon_share_elec": "low_carbon_electricity_share_pct",
    "low_carbon_share_energy": "low_carbon_energy_share_pct",
    "net_elec_imports": "electricity_net_imports_twh",
    "net_elec_imports_share_demand": "electricity_net_imports_share_of_demand_pct",
    "nuclear_cons_change_pct": "nuclear_energy_annual_change_pct",
    "nuclear_cons_change_twh": "nuclear_energy_annual_change_twh",
    "nuclear_consumption": "nuclear_energy_twh",
    "nuclear_elec_per_capita": "nuclear_electricity_per_capita_kwh",
    "nuclear_electricity": "nuclear_electricity_twh",
    "nuclear_energy_per_capita": "nuclear_energy_per_capita_kwh",
    "nuclear_share_elec": "nuclear_electricity_share_pct",
    "nuclear_share_energy": "nuclear_energy_share_pct",
    "oil_cons_change_pct": "oil_energy_annual_change_pct",
    "oil_cons_change_twh": "oil_energy_annual_change_twh",
    "oil_consumption": "oil_energy_twh",
    "oil_elec_per_capita": "oil_electricity_per_capita_kwh",
    "oil_electricity": "oil_electricity_twh",
    "oil_energy_per_capita": "oil_energy_per_capita_kwh",
    "oil_prod_change_pct": "oil_production_annual_change_pct",
    "oil_prod_change_twh": "oil_production_annual_change_twh",
    "oil_prod_per_capita": "oil_production_per_capita_kwh",
    "oil_production": "oil_production_twh",
    "oil_share_elec": "oil_electricity_share_pct",
    "oil_share_energy": "oil_energy_share_pct",
    "other_renewable_consumption": "other_renewables_energy_twh",
    "other_renewable_electricity": "other_renewables_including_bioenergy_electricity_twh",
    "other_renewable_exc_biofuel_electricity": "other_renewables_excluding_bioenergy_electricity_twh",
    "other_renewables_cons_change_pct": "other_renewables_energy_annual_change_pct",
    "other_renewables_cons_change_twh": "other_renewables_energy_annual_change_twh",
    "other_renewables_elec_per_capita": "other_renewables_including_bioenergy_electricity_per_capita_kwh",
    "other_renewables_elec_per_capita_exc_biofuel": "other_renewables_excluding_bioenergy_electricity_per_capita_kwh",
    "other_renewables_energy_per_capita": "other_renewables_energy_per_capita_kwh",
    "other_renewables_share_elec": "other_renewables_including_bioenergy_electricity_share_pct",
    "other_renewables_share_elec_exc_biofuel": "other_renewables_excluding_bioenergy_electricity_share_pct",
    "other_renewables_share_energy": "other_renewables_energy_share_pct",
    "per_capita_electricity": "electricity_generation_per_capita_kwh",
    "primary_energy_consumption": "total_energy_supply_twh",
    "renewables_cons_change_pct": "renewables_energy_annual_change_pct",
    "renewables_cons_change_twh": "renewables_energy_annual_change_twh",
    "renewables_consumption": "renewables_energy_twh",
    "renewables_elec_per_capita": "renewables_electricity_per_capita_kwh",
    "renewables_electricity": "renewables_electricity_twh",
    "renewables_energy_per_capita": "renewables_energy_per_capita_kwh",
    "renewables_share_elec": "renewables_electricity_share_pct",
    "renewables_share_energy": "renewables_energy_share_pct",
    "solar_cons_change_pct": "solar_energy_annual_change_pct",
    "solar_cons_change_twh": "solar_energy_annual_change_twh",
    "solar_consumption": "solar_energy_twh",
    "solar_elec_per_capita": "solar_electricity_per_capita_kwh",
    "solar_electricity": "solar_electricity_twh",
    "solar_energy_per_capita": "solar_energy_per_capita_kwh",
    "solar_share_elec": "solar_electricity_share_pct",
    "solar_share_energy": "solar_energy_share_pct",
    "wind_cons_change_pct": "wind_energy_annual_change_pct",
    "wind_cons_change_twh": "wind_energy_annual_change_twh",
    "wind_consumption": "wind_energy_twh",
    "wind_elec_per_capita": "wind_electricity_per_capita_kwh",
    "wind_electricity": "wind_electricity_twh",
    "wind_energy_per_capita": "wind_energy_per_capita_kwh",
    "wind_share_elec": "wind_electricity_share_pct",
    "wind_share_energy": "wind_energy_share_pct",
}


def rename_columns(tb: Table, columns: dict[str, str | None]) -> Table:
    """Keep and rename the columns listed in the dictionary; fail if the table has a column not listed."""
    unexpected = set(tb.columns) - set(columns) - {"country", "year"}
    assert not unexpected, f"Columns not listed in the mapping: {sorted(unexpected)}"
    missing = set(columns) - set(tb.columns)
    assert not missing, f"Columns listed in the mapping but not in the table: {sorted(missing)}"
    kept = {old: new for old, new in columns.items() if new is not None}
    return tb[["country", "year"] + list(kept)].rename(columns=kept, errors="raise")


def sanity_check(tb: Table) -> None:
    columns = tb.columns
    assert list(columns[: len(CONTEXT_COLUMNS)]) == CONTEXT_COLUMNS, "Context columns should come first."
    assert len(set(columns)) == len(columns), "Repeated column names."
    assert not columns.str.contains("__").any(), "Double underscores in column names."
    assert (columns == columns.str.lower()).all(), "Column names should be lowercase."
    for abbreviation in ["_cons_", "_elec_", "_prod_", "_exc_"]:
        assert not columns.str.contains(abbreviation).any(), f"Abbreviation {abbreviation} in column names."
    missing = set(PREVIOUS_COLUMNS.values()) - set(columns)
    assert not missing, f"Previous columns mapped to columns that do not exist: {sorted(missing)}"

    numeric_columns = tb.select_dtypes("number").columns
    columns_with_inf = [column for column in numeric_columns if np.isinf(tb[column].astype(float)).any()]
    assert not columns_with_inf, f"Infinity values in columns: {columns_with_inf}"
    rows_without_data = tb.drop(columns=CONTEXT_COLUMNS).isnull().all(axis=1)
    assert rows_without_data.sum() == 0, f"{rows_without_data.sum()} rows have no data."
    assert tb.codebook["column"].tolist() == tb.columns.tolist(), "Codebook and data columns differ."
    for old_name in ["burma", "macedonia", "swaziland", "czech republic"]:
        assert old_name not in set(tb["country"].str.lower()), f"Deprecated country name: {old_name}"
    # Values that pin the methodology: on the total energy supply basis, the World's hydro is far below the
    # substitution-method figure (about 4,300 TWh in 2024 instead of about 11,000 TWh).
    world_2024 = tb[(tb["country"] == "World") & (tb["year"] == 2024)]
    assert len(world_2024) == 1
    assert 3000 < world_2024["hydro_energy_twh"].item() < 6000, "World hydro 2024 is not on the TES basis."
    assert 150000 < world_2024["total_energy_supply_twh"].item() < 190000


def run() -> None:
    #
    # Load data.
    #
    tb_energy_mix = paths.load_dataset("energy_mix").read("energy_mix")
    # The electricity mix dataset also has a monthly table; only the annual one is used.
    tb_electricity_mix = paths.load_dataset("electricity_mix").read("electricity_mix")
    tb_fossil_fuels = paths.load_dataset("fossil_fuels").read("fossil_fuels")
    ds_gdp = paths.load_dataset("maddison_project_database")
    ds_regions = paths.load_dataset("regions")

    #
    # Process data.
    #
    tb_energy_mix = rename_columns(tb_energy_mix, ENERGY_MIX_COLUMNS)
    tb_electricity_mix = rename_columns(tb_electricity_mix, ELECTRICITY_MIX_COLUMNS)
    tb_fossil_fuels = rename_columns(tb_fossil_fuels, FOSSIL_FUELS_COLUMNS)
    tb = pr.multi_merge([tb_energy_mix, tb_electricity_mix, tb_fossil_fuels], on=["country", "year"], how="outer")

    # Add ISO codes (empty for regions and other aggregates), population and GDP.
    tb_regions = ds_regions["regions"].reset_index()[["name", "iso_alpha3"]]
    tb_regions = tb_regions.rename(columns={"name": "country", "iso_alpha3": "iso_code"})
    tb = pr.merge(tb, tb_regions, on="country", how="left")
    tb = paths.regions.add_population(tb=tb, warn_on_missing_countries=False)
    tb = add_gdp_to_table(tb=tb, ds_gdp=ds_gdp)

    # Context columns first, then everything else in alphabetical order; drop rows without any data.
    data_columns = sorted(column for column in tb.columns if column not in CONTEXT_COLUMNS)
    tb = tb[CONTEXT_COLUMNS + data_columns]
    tb = tb.dropna(subset=data_columns, how="all").reset_index(drop=True)

    # Metadata of the context columns.
    regions_origin = Origin(producer="Our World in Data", title="Regions", date_published=ds_regions.metadata.version)
    tb["country"].metadata.title = "Country"
    tb["country"].metadata.description_short = "Country or region."
    tb["country"].metadata.unit = ""
    tb["country"].metadata.origins = [regions_origin]
    tb["year"].metadata.title = "Year"
    tb["year"].metadata.description_short = "Year of observation."
    tb["year"].metadata.unit = ""
    tb["year"].metadata.origins = [regions_origin]
    tb["iso_code"].metadata.title = "ISO code"
    tb[
        "iso_code"
    ].metadata.description_short = (
        "ISO 3166-1 alpha-3 three-letter country code. Empty for regions and other aggregates."
    )
    tb["iso_code"].metadata.unit = ""
    tb["iso_code"].metadata.origins = [
        Origin(
            producer="International Organization for Standardization",
            title="ISO 3166 Country Codes",
            date_published=ds_regions.metadata.version,
            url_main="https://www.iso.org/iso-3166-country-codes.html",
        )
    ]

    sanity_check(tb)

    # Table with the name each column had in the previous release.
    previous_names = {new: old for old, new in PREVIOUS_COLUMNS.items()}
    tb_mapping = Table(
        {
            "column": list(tb.columns),
            "previous_column": [previous_names.get(column, "") for column in tb.columns],
        },
        short_name="column_mapping",
    )
    tb_mapping["status"] = "new"
    tb_mapping.loc[tb_mapping["previous_column"] != "", "status"] = "renamed"
    tb_mapping.loc[tb_mapping["previous_column"] == tb_mapping["column"], "status"] = "unchanged"
    tb_mapping["column"].metadata.title = "Column"
    tb_mapping["column"].metadata.description_short = "Name of the column in the current release."
    tb_mapping["previous_column"].metadata.title = "Previous column"
    tb_mapping[
        "previous_column"
    ].metadata.description_short = "Name of the column in the previous release. Empty for new columns."
    tb_mapping["status"].metadata.title = "Status"
    tb_mapping["status"].metadata.description_short = 'One of "unchanged", "renamed" or "new".'
    for column in tb_mapping.columns:
        tb_mapping[column].metadata.unit = ""
        tb_mapping[column].metadata.origins = []

    tb = tb.format(["country", "year"], short_name=paths.short_name, sort_columns=False)
    tb_mapping = tb_mapping.format(["column"], short_name="column_mapping", sort_columns=False)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb, tb_mapping])
    ds_garden.save()
