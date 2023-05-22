"""Garden step that combines various datasets related to greenhouse emissions and produces the OWID CO2 dataset (2022).

Datasets combined:
* Global Carbon Budget (Global Carbon Project, 2022).
* Greenhouse gas emissions by sector (CAIT, 2022).
* Primary energy consumption (BP & EIA, 2022)

Additionally, OWID's population dataset and Maddison Project Database (Bolt and van Zanden, 2020) on GDP are included.

"""

from typing import cast

import numpy as np
import pandas as pd
from owid import catalog
from owid.datautils import dataframes
from shared import CURRENT_DIR, gather_sources_from_tables

from etl.helpers import PathFinder
from etl.paths import DATA_DIR

paths = PathFinder(__file__)

# Details for dataset to export.
DATASET_SHORT_NAME = "owid_co2"
DATASET_TITLE = "CO2 dataset (OWID, 2022)"
METADATA_PATH = CURRENT_DIR / f"{DATASET_SHORT_NAME}.meta.yml"
# Details for datasets to import.
GCP_PATH = DATA_DIR / "garden/gcp/2022-11-11/global_carbon_budget"
CAIT_PATH = DATA_DIR / "garden/cait/2022-08-10/ghg_emissions_by_sector"
PRIMARY_ENERGY_PATH = DATA_DIR / "garden/energy/2022-07-29/primary_energy_consumption"
# Population is only used to add the population column (and no other derived variables).
POPULATION_PATH = DATA_DIR / "garden/owid/latest/key_indicators/"
# GDP is only used to add the gdp column (and no other derived variables).
GDP_PATH = DATA_DIR / "garden/ggdc/2020-10-01/ggdc_maddison"

# Conversion factor from tonnes to million tonnes.
TONNES_TO_MILLION_TONNES = 1e-6

# Select columns to use from each dataset, and how to rename them.
GCP_COLUMNS = {
    "country": "country",
    "year": "year",
    "emissions_total": "co2",
    "emissions_total_per_capita": "co2_per_capita",
    "traded_emissions": "trade_co2",
    "emissions_from_cement": "cement_co2",
    "emissions_from_cement_per_capita": "cement_co2_per_capita",
    "emissions_from_coal": "coal_co2",
    "emissions_from_coal_per_capita": "coal_co2_per_capita",
    "emissions_from_flaring": "flaring_co2",
    "emissions_from_flaring_per_capita": "flaring_co2_per_capita",
    "emissions_from_gas": "gas_co2",
    "emissions_from_gas_per_capita": "gas_co2_per_capita",
    "emissions_from_oil": "oil_co2",
    "emissions_from_oil_per_capita": "oil_co2_per_capita",
    "emissions_from_other_industry": "other_industry_co2",
    "emissions_from_other_industry_per_capita": "other_co2_per_capita",
    "pct_growth_emissions_total": "co2_growth_prct",
    "growth_emissions_total": "co2_growth_abs",
    "emissions_total_per_gdp": "co2_per_gdp",
    "emissions_total_per_unit_energy": "co2_per_unit_energy",
    "consumption_emissions": "consumption_co2",
    "consumption_emissions_per_capita": "consumption_co2_per_capita",
    "consumption_emissions_per_gdp": "consumption_co2_per_gdp",
    "cumulative_emissions_total": "cumulative_co2",
    "cumulative_emissions_from_cement": "cumulative_cement_co2",
    "cumulative_emissions_from_coal": "cumulative_coal_co2",
    "cumulative_emissions_from_flaring": "cumulative_flaring_co2",
    "cumulative_emissions_from_gas": "cumulative_gas_co2",
    "cumulative_emissions_from_oil": "cumulative_oil_co2",
    "cumulative_emissions_from_other_industry": "cumulative_other_co2",
    "pct_traded_emissions": "trade_co2_share",
    "emissions_total_as_share_of_global": "share_global_co2",
    "emissions_from_cement_as_share_of_global": "share_global_cement_co2",
    "emissions_from_coal_as_share_of_global": "share_global_coal_co2",
    "emissions_from_flaring_as_share_of_global": "share_global_flaring_co2",
    "emissions_from_gas_as_share_of_global": "share_global_gas_co2",
    "emissions_from_oil_as_share_of_global": "share_global_oil_co2",
    "emissions_from_other_industry_as_share_of_global": "share_global_other_co2",
    "cumulative_emissions_total_as_share_of_global": "share_global_cumulative_co2",
    "cumulative_emissions_from_cement_as_share_of_global": "share_global_cumulative_cement_co2",
    "cumulative_emissions_from_coal_as_share_of_global": "share_global_cumulative_coal_co2",
    "cumulative_emissions_from_flaring_as_share_of_global": "share_global_cumulative_flaring_co2",
    "cumulative_emissions_from_gas_as_share_of_global": "share_global_cumulative_gas_co2",
    "cumulative_emissions_from_oil_as_share_of_global": "share_global_cumulative_oil_co2",
    "cumulative_emissions_from_other_industry_as_share_of_global": "share_global_cumulative_other_co2",
    # New variables, related to land-use change emissions.
    "cumulative_emissions_from_land_use_change": "cumulative_luc_co2",
    "cumulative_emissions_from_land_use_change_as_share_of_global": "share_global_cumulative_luc_co2",
    "cumulative_emissions_total_including_land_use_change": "cumulative_co2_including_luc",
    "cumulative_emissions_total_including_land_use_change_as_share_of_global": "share_global_cumulative_co2_including_luc",
    "emissions_from_land_use_change": "land_use_change_co2",
    "emissions_from_land_use_change_as_share_of_global": "share_global_luc_co2",
    "emissions_from_land_use_change_per_capita": "land_use_change_co2_per_capita",
    "emissions_total_including_land_use_change": "co2_including_luc",
    "emissions_total_including_land_use_change_as_share_of_global": "share_global_co2_including_luc",
    "emissions_total_including_land_use_change_per_capita": "co2_including_luc_per_capita",
    "emissions_total_including_land_use_change_per_gdp": "co2_including_luc_per_gdp",
    "emissions_total_including_land_use_change_per_unit_energy": "co2_including_luc_per_unit_energy",
    "growth_emissions_total_including_land_use_change": "co2_including_luc_growth_abs",
    "pct_growth_emissions_total_including_land_use_change": "co2_including_luc_growth_prct",
}
CAIT_GHG_COLUMNS = {
    "country": "country",
    "year": "year",
    "total_excluding_lucf": "total_ghg_excluding_lucf",
    "total_excluding_lucf__per_capita": "ghg_excluding_lucf_per_capita",
    "total_including_lucf": "total_ghg",
    "total_including_lucf__per_capita": "ghg_per_capita",
}
CAIT_CH4_COLUMNS = {
    "country": "country",
    "year": "year",
    "total_including_lucf": "methane",
    "total_including_lucf__per_capita": "methane_per_capita",
}
CAIT_N2O_COLUMNS = {
    "country": "country",
    "year": "year",
    "total_including_lucf": "nitrous_oxide",
    "total_including_lucf__per_capita": "nitrous_oxide_per_capita",
}
PRIMARY_ENERGY_COLUMNS = {
    "country": "country",
    "year": "year",
    "primary_energy_consumption__twh": "primary_energy_consumption",
    "primary_energy_consumption_per_capita__kwh": "energy_per_capita",
    "primary_energy_consumption_per_gdp__kwh_per_dollar": "energy_per_gdp",
}
COUNTRIES_REGIONS_COLUMNS = {
    "name": "country",
    "code": "iso_code",
}
POPULATION_COLUMNS = {
    "country": "country",
    "year": "year",
    "population": "population",
}
GDP_COLUMNS = {
    "country": "country",
    "year": "year",
    "gdp": "gdp",
}

UNITS = {"tonnes": {"conversion": TONNES_TO_MILLION_TONNES, "new_unit": "million tonnes"}}


def convert_units(table: catalog.Table) -> catalog.Table:
    """Convert units of table.

    Parameters
    ----------
    table : catalog.Table
        Data with its original units.

    Returns
    -------
    catalog.Table
        Data after converting units of specific columns.

    """
    table = table.copy()
    # Check units and convert to more convenient ones.
    for column in table.columns:
        unit = table[column].metadata.unit
        if unit in list(UNITS):
            table[column] *= UNITS[unit]["conversion"]
            table[column].metadata.description = table[column].metadata.description.replace(
                unit, UNITS[unit]["new_unit"]
            )

    return table


def combine_tables(
    tb_gcp: catalog.Table,
    tb_cait_ghg: catalog.Table,
    tb_cait_ch4: catalog.Table,
    tb_cait_n2o: catalog.Table,
    tb_energy: catalog.Table,
    tb_gdp: catalog.Table,
    tb_population: catalog.Table,
    tb_countries_regions: catalog.Table,
) -> catalog.Table:
    """Combine tables.

    Parameters
    ----------
    tb_gcp : catalog.Table
        Global Carbon Budget table (from Global Carbon Project).
    tb_cait_ghg : catalog.Table
        Greenhouse gas emissions table (from CAIT).
    tb_cait_ch4 : catalog.Table
        CH4 emissions table (from CAIT).
    tb_cait_n2o : catalog.Table
        N2O emissions table (from CAIT).
    tb_energy : catalog.Table
        Primary energy consumption table (from BP & EIA).
    tb_gdp : catalog.Table
        Maddison GDP table (from GGDC).
    tb_population : catalog.Table
        OWID population table (from various sources).
    tb_countries_regions : catalog.Table
        OWID countries-regions table.

    Returns
    -------
    combined : catalog.Table
        Combined table with metadata and variables metadata.

    """
    # Gather all variables' metadata from all tables.
    tables = [tb_gcp, tb_cait_ghg, tb_cait_ch4, tb_cait_n2o, tb_energy, tb_gdp, tb_population, tb_countries_regions]
    variables_metadata = {}
    for table in tables:
        for variable in table.columns:
            # If variable does not have sources metadata, take them from the dataset metadata.
            if len(table[variable].metadata.sources) == 0:
                if type(table.metadata.dataset) == dict:
                    # This happens for countries-regions, which has a different metadata format.
                    table[variable].metadata.sources = []
                else:
                    table[variable].metadata.sources = table.metadata.dataset.sources
            variables_metadata[variable] = table[variable].metadata

    # Combine main tables (with an outer join, to gather all entities from all tables).
    tables = [tb_gcp, tb_cait_ghg, tb_cait_ch4, tb_cait_n2o]
    combined = dataframes.multi_merge(dfs=tables, on=["country", "year"], how="outer")

    # Add secondary tables (with a left join, to keep only entities for which we have emissions data).
    tables = [combined, tb_energy, tb_gdp, tb_population]
    combined = dataframes.multi_merge(dfs=tables, on=["country", "year"], how="left")

    # Countries-regions dataset does not have a year column, so it has to be merged on country.
    combined = pd.merge(combined, tb_countries_regions, on="country", how="left")

    # OWID population dataset does not have sources metadata.
    # Add those sources manually.
    tb_population.metadata.dataset = catalog.meta.DatasetMeta(
        sources=[
            catalog.meta.Source(
                name="Our World in Data based on different sources (https://ourworldindata.org/population-sources)."
            )
        ]
    )

    # Assign variables metadata back to combined dataframe.
    for variable in variables_metadata:
        combined[variable].metadata = variables_metadata[variable]

    # Check that there were no repetition in column names.
    error = "Repeated columns in combined data."
    assert len([column for column in set(combined.columns) if "_x" in column]) == 0, error

    # Adjust units.
    combined = convert_units(combined)

    return combined


def prepare_outputs(combined: catalog.Table) -> catalog.Table:
    """Clean and prepare output table.

    Parameters
    ----------
    combined : catalog.Table
        Combined table.

    Returns
    -------
    combined: catalog.Table
        Cleaned combined table.

    """
    # Remove rows that only have nan (ignoring if country, year, iso_code, population and gdp do have data).
    columns_that_must_have_data = [
        column for column in combined.columns if column not in ["country", "year", "iso_code", "population", "gdp"]
    ]
    combined = combined.dropna(subset=columns_that_must_have_data, how="all").reset_index(drop=True)

    # Sanity check.
    columns_with_inf = [column for column in combined.columns if len(combined[combined[column] == np.inf]) > 0]
    assert len(columns_with_inf) == 0, f"Infinity values detected in columns: {columns_with_inf}"

    # Set index and sort conveniently.
    combined = combined.set_index(["country", "year"], verify_integrity=True).sort_index()

    return combined


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Read all required datasets.
    # Load the global carbon budget dataset from the Global Carbon Project (GCP).
    ds_gcp = catalog.Dataset(GCP_PATH)
    # Load the greenhouse gas emissions by sector dataset by CAIT.
    ds_cait = catalog.Dataset(CAIT_PATH)
    # Load the GDP dataset by GGDC Maddison.
    ds_gdp = catalog.Dataset(GDP_PATH)
    # Load primary energy consumption dataset (by different sources in our 'energy' namespace).
    ds_energy = catalog.Dataset(PRIMARY_ENERGY_PATH)
    # Load population dataset.
    ds_population = catalog.Dataset(POPULATION_PATH)

    # Gather all required tables from all datasets.
    tb_gcp = ds_gcp[ds_gcp.table_names[0]]
    tb_cait_ghg = ds_cait["greenhouse_gas_emissions_by_sector"]
    tb_cait_ch4 = ds_cait["methane_emissions_by_sector"]
    tb_cait_n2o = ds_cait["nitrous_oxide_emissions_by_sector"]
    tb_energy = ds_energy["primary_energy_consumption"]
    tb_gdp = ds_gdp["maddison_gdp"]
    tb_population = ds_population["population"]
    tb_countries_regions = cast(catalog.Dataset, paths.load_dependency("regions"))["regions"]
    tb_countries_regions["code"] = tb_countries_regions.index

    #
    # Process data.
    #
    # Choose required columns and rename them.
    tb_gcp = tb_gcp.reset_index()[list(GCP_COLUMNS)].rename(columns=GCP_COLUMNS)
    tb_cait_ghg = tb_cait_ghg.reset_index()[list(CAIT_GHG_COLUMNS)].rename(columns=CAIT_GHG_COLUMNS)
    tb_cait_ch4 = tb_cait_ch4.reset_index()[list(CAIT_CH4_COLUMNS)].rename(columns=CAIT_CH4_COLUMNS)
    tb_cait_n2o = tb_cait_n2o.reset_index()[list(CAIT_N2O_COLUMNS)].rename(columns=CAIT_N2O_COLUMNS)
    tb_energy = tb_energy.reset_index()[list(PRIMARY_ENERGY_COLUMNS)].rename(columns=PRIMARY_ENERGY_COLUMNS)
    tb_gdp = tb_gdp.reset_index()[list(GDP_COLUMNS)].rename(columns=GDP_COLUMNS)
    tb_population = tb_population.reset_index()[list(POPULATION_COLUMNS)].rename(columns=POPULATION_COLUMNS)
    tb_countries_regions = tb_countries_regions.reset_index()[list(COUNTRIES_REGIONS_COLUMNS)].rename(
        columns=COUNTRIES_REGIONS_COLUMNS
    )

    # Combine tables.
    combined = combine_tables(
        tb_gcp=tb_gcp,
        tb_cait_ghg=tb_cait_ghg,
        tb_cait_ch4=tb_cait_ch4,
        tb_cait_n2o=tb_cait_n2o,
        tb_energy=tb_energy,
        tb_gdp=tb_gdp,
        tb_population=tb_population,
        tb_countries_regions=tb_countries_regions,
    )

    # Prepare outputs.
    combined = prepare_outputs(combined=combined)

    #
    # Save outputs.
    #
    ds_garden = catalog.Dataset.create_empty(dest_dir)
    # Gather metadata sources from all tables' original dataset sources.
    tables = [tb_gcp, tb_cait_ghg, tb_cait_ch4, tb_cait_n2o, tb_energy, tb_gdp, tb_population]
    ds_garden.metadata.sources = gather_sources_from_tables(tables=tables)

    # Get the rest of the metadata from the yaml file.
    ds_garden.metadata.update_from_yaml(METADATA_PATH)
    # Create dataset.
    ds_garden.save()

    # Add other metadata fields to table.
    combined.metadata.short_name = DATASET_SHORT_NAME
    combined.metadata.title = DATASET_TITLE
    combined.metadata.dataset = ds_garden.metadata

    # Add combined tables to the new dataset.
    ds_garden.add(combined)
