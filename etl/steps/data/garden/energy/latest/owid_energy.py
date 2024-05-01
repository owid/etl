"""Garden step that combines various datasets related to energy and produces the OWID Energy dataset.

Datasets combined:
* Energy mix from the Energy Institute (EI) Statistical Review of World Energy.
* Fossil fuel production (EI & Shift).
* Primary energy consumption (EI & EIA).
* Electricity mix (EI & Ember).

Auxiliary datasets:
* Regions (OWID).
* Population (OWID based on various sources).
* GDP (GGDC Maddison).

NOTE: Currently we define all metadata in the companion file owid_energy_variable_mapping.csv.
In the future we may want to avoid it, and use the propagated metadata, without having to manually overwrite it.

"""

from typing import Dict

import numpy as np
import owid.catalog.processing as pr
from owid.catalog import Dataset, Origin, Table

from etl.data_helpers.geo import add_gdp_to_table, add_population_to_table
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Mapping from original column names to new column names.
COLUMNS = {
    "country": "country",
    "year": "year",
    "iso_alpha3": "iso_code",
    "population": "population",
    "gdp": "gdp",
    "biofuels__pct_growth": "biofuel_cons_change_pct",
    "biofuels__twh_growth": "biofuel_cons_change_twh",
    "biofuels_per_capita__kwh": "biofuel_cons_per_capita",
    "biofuels__twh": "biofuel_consumption",
    "per_capita_bioenergy_generation__kwh": "biofuel_elec_per_capita",
    "bioenergy_generation__twh": "biofuel_electricity",
    "bioenergy_share_of_electricity__pct": "biofuel_share_elec",
    "biofuels__pct_equivalent_primary_energy": "biofuel_share_energy",
    "co2_intensity__gco2_kwh": "carbon_intensity_elec",
    "coal__pct_growth": "coal_cons_change_pct",
    "coal__twh_growth": "coal_cons_change_twh",
    "coal_per_capita__kwh": "coal_cons_per_capita",
    "coal__twh": "coal_consumption",
    "per_capita_coal_generation__kwh": "coal_elec_per_capita",
    "coal_generation__twh": "coal_electricity",
    "annual_change_in_coal_production__pct": "coal_prod_change_pct",
    "annual_change_in_coal_production__twh": "coal_prod_change_twh",
    "coal_production_per_capita__kwh": "coal_prod_per_capita",
    "coal_production__twh": "coal_production",
    "coal_share_of_electricity__pct": "coal_share_elec",
    "coal__pct_equivalent_primary_energy": "coal_share_energy",
    "total_demand__twh": "electricity_demand",
    "total_generation__twh": "electricity_generation",
    "total_electricity_share_of_primary_energy__pct": "electricity_share_energy",
    "annual_change_in_primary_energy_consumption__pct": "energy_cons_change_pct",
    "annual_change_in_primary_energy_consumption__twh": "energy_cons_change_twh",
    "primary_energy_consumption_per_capita__kwh": "energy_per_capita",
    "primary_energy_consumption_per_gdp__kwh_per_dollar": "energy_per_gdp",
    "fossil_fuels__pct_growth": "fossil_cons_change_pct",
    "fossil_fuels__twh_growth": "fossil_cons_change_twh",
    "per_capita_fossil_generation__kwh": "fossil_elec_per_capita",
    "fossil_generation__twh": "fossil_electricity",
    "fossil_fuels_per_capita__kwh": "fossil_energy_per_capita",
    "fossil_fuels__twh": "fossil_fuel_consumption",
    "fossil_share_of_electricity__pct": "fossil_share_elec",
    "fossil_fuels__pct_equivalent_primary_energy": "fossil_share_energy",
    "gas__pct_growth": "gas_cons_change_pct",
    "gas__twh_growth": "gas_cons_change_twh",
    "gas__twh": "gas_consumption",
    "per_capita_gas_generation__kwh": "gas_elec_per_capita",
    "gas_generation__twh": "gas_electricity",
    "gas_per_capita__kwh": "gas_energy_per_capita",
    "annual_change_in_gas_production__pct": "gas_prod_change_pct",
    "annual_change_in_gas_production__twh": "gas_prod_change_twh",
    "gas_production_per_capita__kwh": "gas_prod_per_capita",
    "gas_production__twh": "gas_production",
    "gas_share_of_electricity__pct": "gas_share_elec",
    "gas__pct_equivalent_primary_energy": "gas_share_energy",
    "total_emissions__mtco2": "greenhouse_gas_emissions",
    "hydro__pct_growth": "hydro_cons_change_pct",
    "hydro__twh_growth__equivalent": "hydro_cons_change_twh",
    "hydro__twh__equivalent": "hydro_consumption",
    "per_capita_hydro_generation__kwh": "hydro_elec_per_capita",
    "hydro_generation__twh": "hydro_electricity",
    "hydro_per_capita__kwh__equivalent": "hydro_energy_per_capita",
    "hydro_share_of_electricity__pct": "hydro_share_elec",
    "hydro__pct_equivalent_primary_energy": "hydro_share_energy",
    "low_carbon_energy__pct_growth": "low_carbon_cons_change_pct",
    "low_carbon_energy__twh_growth__equivalent": "low_carbon_cons_change_twh",
    "low_carbon_energy__twh__equivalent": "low_carbon_consumption",
    "per_capita_low_carbon_generation__kwh": "low_carbon_elec_per_capita",
    "low_carbon_generation__twh": "low_carbon_electricity",
    "low_carbon_energy_per_capita__kwh__equivalent": "low_carbon_energy_per_capita",
    "low_carbon_share_of_electricity__pct": "low_carbon_share_elec",
    "low_carbon_energy__pct_equivalent_primary_energy": "low_carbon_share_energy",
    "total_net_imports__twh": "net_elec_imports",
    "net_imports_share_of_demand__pct": "net_elec_imports_share_demand",
    "nuclear__pct_growth": "nuclear_cons_change_pct",
    "nuclear__twh_growth__equivalent": "nuclear_cons_change_twh",
    "nuclear__twh__equivalent": "nuclear_consumption",
    "per_capita_nuclear_generation__kwh": "nuclear_elec_per_capita",
    "nuclear_generation__twh": "nuclear_electricity",
    "nuclear_per_capita__kwh__equivalent": "nuclear_energy_per_capita",
    "nuclear_share_of_electricity__pct": "nuclear_share_elec",
    "nuclear__pct_equivalent_primary_energy": "nuclear_share_energy",
    "oil__pct_growth": "oil_cons_change_pct",
    "oil__twh_growth": "oil_cons_change_twh",
    "oil__twh": "oil_consumption",
    "per_capita_oil_generation__kwh": "oil_elec_per_capita",
    "oil_generation__twh": "oil_electricity",
    "oil_per_capita__kwh": "oil_energy_per_capita",
    "annual_change_in_oil_production__pct": "oil_prod_change_pct",
    "annual_change_in_oil_production__twh": "oil_prod_change_twh",
    "oil_production_per_capita__kwh": "oil_prod_per_capita",
    "oil_production__twh": "oil_production",
    "oil_share_of_electricity__pct": "oil_share_elec",
    "oil__pct_equivalent_primary_energy": "oil_share_energy",
    "other_renewables__twh__equivalent": "other_renewable_consumption",
    "other_renewables_including_bioenergy_generation__twh": "other_renewable_electricity",
    "other_renewables_excluding_bioenergy_generation__twh": "other_renewable_exc_biofuel_electricity",
    "other_renewables__pct_growth": "other_renewables_cons_change_pct",
    "other_renewables__twh_growth__equivalent": "other_renewables_cons_change_twh",
    "per_capita_other_renewables_including_bioenergy_generation__kwh": "other_renewables_elec_per_capita",
    "per_capita_other_renewables_excluding_bioenergy_generation__kwh": "other_renewables_elec_per_capita_exc_biofuel",
    "other_renewables_per_capita__kwh__equivalent": "other_renewables_energy_per_capita",
    "other_renewables_including_bioenergy_share_of_electricity__pct": "other_renewables_share_elec",
    "other_renewables_excluding_bioenergy_share_of_electricity__pct": "other_renewables_share_elec_exc_biofuel",
    "other_renewables__pct_equivalent_primary_energy": "other_renewables_share_energy",
    "per_capita_total_generation__kwh": "per_capita_electricity",
    "primary_energy_consumption__twh": "primary_energy_consumption",
    "renewables__pct_growth": "renewables_cons_change_pct",
    "renewables__twh_growth__equivalent": "renewables_cons_change_twh",
    "renewables__twh__equivalent": "renewables_consumption",
    "per_capita_renewable_generation__kwh": "renewables_elec_per_capita",
    "renewable_generation__twh": "renewables_electricity",
    "renewables_per_capita__kwh__equivalent": "renewables_energy_per_capita",
    "renewable_share_of_electricity__pct": "renewables_share_elec",
    "renewables__pct_equivalent_primary_energy": "renewables_share_energy",
    "solar__pct_growth": "solar_cons_change_pct",
    "solar__twh_growth__equivalent": "solar_cons_change_twh",
    "solar__twh__equivalent": "solar_consumption",
    "per_capita_solar_generation__kwh": "solar_elec_per_capita",
    "solar_generation__twh": "solar_electricity",
    "solar_per_capita__kwh__equivalent": "solar_energy_per_capita",
    "solar_share_of_electricity__pct": "solar_share_elec",
    "solar__pct_equivalent_primary_energy": "solar_share_energy",
    "wind__pct_growth": "wind_cons_change_pct",
    "wind__twh_growth__equivalent": "wind_cons_change_twh",
    "wind__twh__equivalent": "wind_consumption",
    "per_capita_wind_generation__kwh": "wind_elec_per_capita",
    "wind_generation__twh": "wind_electricity",
    "wind_per_capita__kwh__equivalent": "wind_energy_per_capita",
    "wind_share_of_electricity__pct": "wind_share_elec",
    "wind__pct_equivalent_primary_energy": "wind_share_energy",
}


def combine_tables_data_and_metadata(
    tables: Dict[str, Table],
    ds_population: Dataset,
    ds_regions: Dataset,
    ds_gdp: Dataset,
) -> Table:
    """Combine data and metadata of a list of tables, map variable names and add variables metadata.

    Parameters
    ----------
    tables : dict
        Dictionary where the key is the short name of the table, and the value is the actual table, for all tables to be
        combined.
    ds_population: Dataset
        Population dataset.
    tb_regions : Table
        Main table from countries-regions dataset.
    tb_gdp: Table
        GDP (from owid catalog, after resetting index, and selecting country, year and gdp
        columns).

    Returns
    -------
    tb_combined : Table
        Combined table with metadata.

    """
    # Merge all tables.
    tb_combined = tables[list(tables)[0]].copy()
    for table_name in list(tables)[1:]:
        tb_combined = pr.merge(
            tb_combined, tables[table_name], on=["country", "year"], how="outer", short_name=paths.short_name
        )

    # Add ISO codes for countries (regions that are not in countries-regions dataset will have nan iso_code).
    tb_combined = pr.merge(
        tb_combined, ds_regions["regions"].reset_index(), left_on="country", right_on="name", how="left"
    )
    # Add metadata to the ISO column (loaded from the regions dataset).
    tb_combined["iso_alpha3"].m.origins = [
        Origin(
            producer="International Organization for Standardization",
            title="Regions",
            date_published=ds_regions.version,
        )
    ]
    tb_combined["iso_alpha3"].metadata.title = "ISO code"
    tb_combined["iso_alpha3"].metadata.description_short = "ISO 3166-1 alpha-3 three-letter country codes."
    tb_combined["iso_alpha3"].metadata.unit = ""

    # Add population and gdp of countries (except for dataset-specific regions e.g. those ending in "(EI)").
    tb_combined = add_population_to_table(tb=tb_combined, ds_population=ds_population, warn_on_missing_countries=False)
    tb_combined = add_gdp_to_table(tb=tb_combined, ds_gdp=ds_gdp)

    # Check that there were no repetition in column names.
    error = "Repeated columns in combined data."
    assert len([column for column in set(tb_combined.columns) if "_x" in column]) == 0, error

    # Select and rename columns.
    tb_combined = tb_combined[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Remove rows that only have nan (ignoring if country, year, iso_code, population and gdp do have data).
    columns_that_must_have_data = [
        column for column in tb_combined.columns if column not in ["country", "year", "iso_code", "population", "gdp"]
    ]
    tb_combined = tb_combined.dropna(subset=columns_that_must_have_data, how="all").reset_index(drop=True)

    # Sanity check.
    columns_with_inf = [column for column in tb_combined.columns if len(tb_combined[tb_combined[column] == np.inf]) > 0]
    assert len(columns_with_inf) == 0, f"Infinity values detected in columns: {columns_with_inf}"

    # Set index and sort conveniently.
    tb_combined = tb_combined.format()

    return tb_combined


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Read all required datasets.
    ds_energy_mix = paths.load_dataset("energy_mix")
    ds_fossil_fuels = paths.load_dataset("fossil_fuel_production")
    ds_primary_energy = paths.load_dataset("primary_energy_consumption")
    ds_electricity_mix = paths.load_dataset("electricity_mix")
    ds_population = paths.load_dataset("population")
    ds_gdp = paths.load_dataset("ggdc_maddison")
    ds_regions = paths.load_dataset("regions")

    # Gather all required tables from all datasets.
    tb_energy_mix = ds_energy_mix["energy_mix"].reset_index()
    tb_fossil_fuels = ds_fossil_fuels["fossil_fuel_production"].reset_index()
    tb_primary_energy = ds_primary_energy["primary_energy_consumption"].reset_index()
    tb_electricity_mix = ds_electricity_mix["electricity_mix"].reset_index()

    #
    # Process data.
    #
    # Combine all tables.
    tables = {
        "energy_mix": tb_energy_mix,
        "fossil_fuel_production": tb_fossil_fuels,
        "primary_energy_consumption": tb_primary_energy.drop(columns=["gdp", "population", "source"], errors="raise"),
        "electricity_mix": tb_electricity_mix.drop(
            columns=["population", "primary_energy_consumption__twh"], errors="raise"
        ),
    }
    tb_combined = combine_tables_data_and_metadata(
        tables=tables,
        ds_population=ds_population,
        ds_regions=ds_regions,
        ds_gdp=ds_gdp,
    )

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_combined], check_variables_metadata=True)
    ds_garden.save()
