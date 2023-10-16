"""Garden step that combines various datasets related to greenhouse emissions and produces the OWID CO2 dataset (2022).

Datasets combined:
* Global Carbon Budget (Global Carbon Project, 2022).
* National contributions to climate change (Jones et al. (2023), 2023).
* Greenhouse gas emissions by sector (CAIT, 2022).
* Primary energy consumption (BP & EIA, 2022)

Additionally, OWID's regions dataset, population dataset and Maddison Project Database (Bolt and van Zanden, 2020) on
GDP are included.

"""

from typing import List

import numpy as np
from owid.catalog import Dataset, Source, Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

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
JONES_COLUMNS = {
    "country": "country",
    "year": "year",
    "temperature_response_co2_total": "temperature_change_from_co2",
    "temperature_response_ghg_total": "temperature_change_from_ghg",
    "temperature_response_ch4_total": "temperature_change_from_ch4",
    "temperature_response_n2o_total": "temperature_change_from_n2o",
    "share_of_temperature_response_ghg_total": "share_of_temperature_change_from_ghg",
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
REGIONS_COLUMNS = {
    "name": "country",
    "iso_alpha3": "iso_code",
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


def unique_sources_from_datasets(
    datasets: List[Dataset],
) -> List[Source]:
    """Gather unique sources from datasets.

    Note: To check if a source is already listed, only the name of the source is considered (not the description or any
    other field in the source).

    Parameters
    ----------
    datasets : list
        List of datasets with metadata.

    Returns
    -------
    known_sources : list
        List of unique sources from all datasets.

    """
    # Initialise list that will gather all unique metadata sources from the tables.
    known_sources: List[Source] = []
    for ds in datasets:
        # Get list of sources of the dataset of current table.
        table_sources = ds.metadata.sources
        # Go source by source of current table, and check if its name is not already in the list of known_sources.
        for source in table_sources:
            # Check if this source's name is different to all known_sources.
            if all([source.name != known_source.name for known_source in known_sources]):
                # Add the new source to the list.
                known_sources.append(source)

    return known_sources


def convert_units(table: Table) -> Table:
    """Convert units of table.

    Parameters
    ----------
    table : Table
        Data with its original units.

    Returns
    -------
    Table
        Data after converting units of specific columns.

    """
    table = table.copy()
    # Check units and convert to more convenient ones.
    for column in table.columns:
        unit = table[column].metadata.unit
        short_unit = table[column].metadata.short_unit
        title = table[column].metadata.title
        description = table[column].metadata.description or table[column].metadata.description_short
        if unit in list(UNITS):
            table[column] *= UNITS[unit]["conversion"]
            table[column].metadata.unit = unit
            table[column].metadata.short_unit = short_unit
            table[column].metadata.title = title
            table[column].metadata.description = description.replace(unit, UNITS[unit]["new_unit"])

    return table


def combine_tables(
    tb_gcp: Table,
    tb_jones: Table,
    tb_cait_ghg: Table,
    tb_cait_ch4: Table,
    tb_cait_n2o: Table,
    tb_energy: Table,
    tb_gdp: Table,
    tb_population: Table,
    tb_regions: Table,
) -> Table:
    """Combine tables.

    Parameters
    ----------
    tb_gcp : Table
        Global Carbon Budget table (from Global Carbon Project).
    tb_jones : Table
        National contributions to climate change (from Jones et al. (2023)).
    tb_cait_ghg : Table
        Greenhouse gas emissions table (from CAIT).
    tb_cait_ch4 : Table
        CH4 emissions table (from CAIT).
    tb_cait_n2o : Table
        N2O emissions table (from CAIT).
    tb_energy : Table
        Primary energy consumption table (from BP & EIA).
    tb_gdp : Table
        Maddison GDP table (from GGDC).
    tb_population : Table
        OWID population table (from various sources).
    tb_regions : Table
        OWID regions table.

    Returns
    -------
    combined : Table
        Combined table with metadata and variables metadata.

    """
    # Combine main tables (with an outer join, to gather all entities from all tables).
    combined = tb_gcp.copy()
    for table in [tb_jones, tb_cait_ghg, tb_cait_ch4, tb_cait_n2o]:
        combined = combined.merge(table, on=["country", "year"], how="outer", short_name=paths.short_name)

    # Add secondary tables (with a left join, to keep only entities for which we have emissions data).
    for table in [tb_energy, tb_gdp, tb_population]:
        combined = combined.merge(table, on=["country", "year"], how="left")

    # Countries-regions dataset does not have a year column, so it has to be merged on country.
    combined = combined.merge(tb_regions, on="country", how="left")

    # Check that there were no repetition in column names.
    error = "Repeated columns in combined data."
    assert len([column for column in set(combined.columns) if "_x" in column]) == 0, error

    # Adjust units.
    combined = convert_units(combined)

    return combined


def prepare_outputs(combined: Table) -> Table:
    """Clean and prepare output table.

    Parameters
    ----------
    combined : Table
        Combined table.

    Returns
    -------
    combined: Table
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
    # Load the global carbon budget dataset from the Global Carbon Project (GCP).
    ds_gcp = paths.load_dataset("global_carbon_budget")

    # Load the Jones et al. (2023) dataset on national contributions to climate change.
    ds_jones = paths.load_dataset("national_contributions")

    # Load the greenhouse gas emissions by sector dataset by CAIT.
    ds_cait = paths.load_dataset("ghg_emissions_by_sector")

    # Load the GDP dataset by GGDC Maddison.
    ds_gdp = paths.load_dataset("ggdc_maddison")

    # Load primary energy consumption dataset (by different sources in our 'energy' namespace).
    ds_energy = paths.load_dataset("primary_energy_consumption")

    # Load population dataset.
    ds_population = paths.load_dataset("population")

    # Load countries-regions dataset (required to get ISO codes).
    ds_regions = paths.load_dataset("regions")

    # Gather all required tables from all datasets.
    tb_gcp = ds_gcp["global_carbon_budget"]
    tb_jones = ds_jones["national_contributions"]
    tb_cait_ghg = ds_cait["greenhouse_gas_emissions_by_sector"]
    tb_cait_ch4 = ds_cait["methane_emissions_by_sector"]
    tb_cait_n2o = ds_cait["nitrous_oxide_emissions_by_sector"]
    tb_energy = ds_energy["primary_energy_consumption"]
    tb_gdp = ds_gdp["maddison_gdp"]
    tb_population = ds_population["population"]
    tb_regions = ds_regions["regions"]

    ####################################################################################################################
    # TODO: Remove this temporary solution once all indicators of all tables of all dataset have metadata.
    def propagate_sources_to_all_indicators(table: Table, ds: Dataset) -> Table:
        table = table.copy()
        for column in table.columns:
            error = f"Column {column} of table {table.metadata.short_name} already has sources or origins. Remove temporary solution for this table."
            assert (len(table[column].metadata.sources) == 0) and (len(table[column].metadata.origins) == 0), error
            # if (len(table[column].metadata.sources) == 0) and (len(table[column].metadata.origins) == 0):
            table[column].metadata.sources = ds.metadata.sources

        return table

    tb_jones = propagate_sources_to_all_indicators(table=tb_jones, ds=ds_jones)
    tb_cait_ghg = propagate_sources_to_all_indicators(table=tb_cait_ghg, ds=ds_cait)
    tb_cait_ch4 = propagate_sources_to_all_indicators(table=tb_cait_ch4, ds=ds_cait)
    tb_cait_n2o = propagate_sources_to_all_indicators(table=tb_cait_n2o, ds=ds_cait)
    tb_regions = propagate_sources_to_all_indicators(table=tb_regions, ds=ds_regions)
    ####################################################################################################################

    #
    # Process data.
    #
    # Choose required columns and rename them.
    tb_gcp = tb_gcp.reset_index()[list(GCP_COLUMNS)].rename(columns=GCP_COLUMNS, errors="raise")
    tb_jones = tb_jones.reset_index()[list(JONES_COLUMNS)].rename(columns=JONES_COLUMNS, errors="raise")
    tb_cait_ghg = tb_cait_ghg.reset_index()[list(CAIT_GHG_COLUMNS)].rename(columns=CAIT_GHG_COLUMNS, errors="raise")
    tb_cait_ch4 = tb_cait_ch4.reset_index()[list(CAIT_CH4_COLUMNS)].rename(columns=CAIT_CH4_COLUMNS, errors="raise")
    tb_cait_n2o = tb_cait_n2o.reset_index()[list(CAIT_N2O_COLUMNS)].rename(columns=CAIT_N2O_COLUMNS, errors="raise")
    tb_energy = tb_energy.reset_index()[list(PRIMARY_ENERGY_COLUMNS)].rename(
        columns=PRIMARY_ENERGY_COLUMNS, errors="raise"
    )
    tb_gdp = tb_gdp.reset_index()[list(GDP_COLUMNS)].rename(columns=GDP_COLUMNS, errors="raise")
    tb_population = tb_population.reset_index()[list(POPULATION_COLUMNS)].rename(
        columns=POPULATION_COLUMNS, errors="raise"
    )
    tb_regions = tb_regions.reset_index()[list(REGIONS_COLUMNS)].rename(columns=REGIONS_COLUMNS, errors="raise")

    # Combine tables.
    combined = combine_tables(
        tb_gcp=tb_gcp,
        tb_jones=tb_jones,
        tb_cait_ghg=tb_cait_ghg,
        tb_cait_ch4=tb_cait_ch4,
        tb_cait_n2o=tb_cait_n2o,
        tb_energy=tb_energy,
        tb_gdp=tb_gdp,
        tb_population=tb_population,
        tb_regions=tb_regions,
    )

    # Prepare outputs.
    combined = prepare_outputs(combined=combined)

    #
    # Save outputs.
    #
    # TODO: Change check_variables_metadata to True once all indicators have origins (for now, they have a mixture of
    # origins and sources).
    ds_garden = create_dataset(dest_dir, tables=[combined], check_variables_metadata=False)
    ds_garden.save()
