"""Garden step that combines various datasets related to energy and produces the OWID Energy dataset (2022).

Datasets combined:
* Energy mix from BP.
* Fossil fuel production (BP & Shift, 2022).
* Primary energy consumption (BP & EIA, 2022).
* Electricity mix (BP & Ember, 2022).

"""

from typing import Dict, cast

import numpy as np
import pandas as pd
from owid import catalog
from owid.datautils import dataframes

from etl.helpers import PathFinder
from etl.paths import DATA_DIR

from .shared import (
    BUILD_POPULATION_FOR_HISTORICAL_COUNTRIES,
    CURRENT_DIR,
    HISTORIC_TO_CURRENT_REGION,
    add_population,
    gather_sources_from_tables,
)

paths = PathFinder(__file__)

# Details for dataset to export.
DATASET_SHORT_NAME = "owid_energy"
DATASET_TITLE = "Energy dataset (OWID, 2022)"
METADATA_PATH = CURRENT_DIR / f"{DATASET_SHORT_NAME}.meta.yml"
# Details for datasets to import.
ENERGY_MIX_DATASET_PATH = DATA_DIR / "garden/bp/2022-07-14/energy_mix"
ENERGY_MIX_TABLE_NAME = "energy_mix"
FOSSIL_FUEL_PRODUCTION_DATASET_PATH = DATA_DIR / "garden/energy/2022-07-20/fossil_fuel_production"
FOSSIL_FUEL_PRODUCTION_TABLE_NAME = "fossil_fuel_production"
PRIMARY_ENERGY_CONSUMPTION_DATASET_PATH = DATA_DIR / "garden/energy/2022-07-29/primary_energy_consumption"
PRIMARY_ENERGY_CONSUMPTION_TABLE_NAME = "primary_energy_consumption"
ELECTRICITY_MIX_DATASET_PATH = DATA_DIR / "garden/energy/2022-12-12/electricity_mix"
ELECTRICITY_MIX_TABLE_NAME = "electricity_mix"
# Population and GDP are only used to add the population and gdp columns (and no other derived variables).
POPULATION_DATASET_PATH = DATA_DIR / "garden/owid/latest/key_indicators/"
GDP_DATASET_PATH = DATA_DIR / "garden/ggdc/2020-10-01/ggdc_maddison"
# Path to file with mapping of variable names from one of the datasets to the final energy dataset.
VARIABLE_MAPPING_FILE = CURRENT_DIR / "owid_energy_variable_mapping.csv"


def combine_tables_data_and_metadata(
    tables: Dict[str, catalog.Table],
    countries_regions: catalog.Table,
    gdp: pd.DataFrame,
    variable_mapping: pd.DataFrame,
) -> catalog.Table:
    """Combine data and metadata of a list of tables, map variable names and add variables metadata.

    Parameters
    ----------
    tables : dict
        Dictionary where the key is the short name of the table, and the value is the actual table, for all tables to be
        combined.
    countries_regions : catalog.Table
        Main table from countries-regions dataset.
    gdp: pd.DataFrame
        GDP (from owid catalog, after converting into a dataframe, resetting index, and selecting country, year and gdp
        columns).
    variable_mapping : pd.DataFrame
        Dataframe (with columns variable, source_variable, source_dataset, description, source) that specifies the names
        of variables to take from each table, and their new name in the output table. It also gives a description of the
        variable, and the sources of the table.

    Returns
    -------
    tb_combined : catalog.Table
        Combined table with metadata.

    """
    # Merge all tables as a dataframe (without metadata).
    dfs = [pd.DataFrame(table) for table in tables.values()]
    df_combined = dataframes.multi_merge(dfs, on=["country", "year"], how="outer")

    # Add ISO codes for countries (regions that are not in countries-regions dataset will have nan iso_code).
    df_combined = pd.merge(df_combined, countries_regions, left_on="country", right_on="name", how="left")

    # Add population and gdp of countries (except for dataset-specific regions e.g. those ending in (BP) or (Shift)).

    historical_regions = {
        region: HISTORIC_TO_CURRENT_REGION[region]
        for region in HISTORIC_TO_CURRENT_REGION
        if region in BUILD_POPULATION_FOR_HISTORICAL_COUNTRIES
    }
    df_combined = add_population(df=df_combined, regions=historical_regions, warn_on_missing_countries=False)
    df_combined = pd.merge(df_combined, gdp, on=["country", "year"], how="left")

    # Check that there were no repetition in column names.
    error = "Repeated columns in combined data."
    assert len([column for column in set(df_combined.columns) if "_x" in column]) == 0, error

    # Create a table with combined data and no metadata.
    tb_combined = catalog.Table(df_combined)

    # List the names of the variables described in the variable mapping file.
    source_variables = variable_mapping.index.get_level_values(0).tolist()

    # Gather original metadata for each variable, add the descriptions and sources from the variable mapping file.
    for source_variable in source_variables:
        variable_metadata = variable_mapping.loc[source_variable]
        source_dataset = variable_metadata["source_dataset"]
        # Check that the variable indeed exists in the original dataset that the variable mapping says.
        # Ignore columns "country", "year" (assigned to a dummy dataset 'various_datasets'), "population" (that comes
        # from key_indicators) and "iso_alpha3" (that comes from countries_regions dataset).
        if source_dataset not in [
            "various_datasets",
            "countries_regions",
            "key_indicators",
            "maddison_gdp",
        ]:
            error = f"Variable {source_variable} not found in any of the original datasets."
            assert source_variable in tables[source_dataset].columns, error
            tb_combined[source_variable].metadata = tables[source_dataset][source_variable].metadata

        # Update metadata with the content of the variable mapping file.
        tb_combined[source_variable].metadata.description = variable_metadata["description"]
        tb_combined[source_variable].metadata.sources = [catalog.meta.Source(name=variable_metadata["source"])]

    # Select only variables in the mapping file, and rename variables according to the mapping.
    tb_combined = tb_combined[source_variables].rename(columns=variable_mapping.to_dict()["variable"])

    # Remove rows that only have nan (ignoring if country, year, iso_code, population and gdp do have data).
    columns_that_must_have_data = [
        column for column in tb_combined.columns if column not in ["country", "year", "iso_code", "population", "gdp"]
    ]
    tb_combined = tb_combined.dropna(subset=columns_that_must_have_data, how="all").reset_index(drop=True)

    # Sanity check.
    columns_with_inf = [column for column in tb_combined.columns if len(tb_combined[tb_combined[column] == np.inf]) > 0]
    assert len(columns_with_inf) == 0, f"Infinity values detected in columns: {columns_with_inf}"

    # Set index and sort conveniently.
    tb_combined = tb_combined.set_index(["country", "year"], verify_integrity=True).sort_index()

    return cast(catalog.Table, tb_combined)


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Read all required datasets.
    ds_energy_mix = catalog.Dataset(ENERGY_MIX_DATASET_PATH)
    ds_fossil_fuels = catalog.Dataset(FOSSIL_FUEL_PRODUCTION_DATASET_PATH)
    ds_primary_energy = catalog.Dataset(PRIMARY_ENERGY_CONSUMPTION_DATASET_PATH)
    ds_electricity_mix = catalog.Dataset(ELECTRICITY_MIX_DATASET_PATH)

    # Gather all required tables from all datasets.
    tb_energy_mix = ds_energy_mix[ENERGY_MIX_TABLE_NAME].reset_index()
    tb_fossil_fuels = ds_fossil_fuels[FOSSIL_FUEL_PRODUCTION_TABLE_NAME].reset_index()
    tb_primary_energy = ds_primary_energy[PRIMARY_ENERGY_CONSUMPTION_TABLE_NAME].reset_index()
    tb_electricity_mix = ds_electricity_mix[ELECTRICITY_MIX_TABLE_NAME].reset_index()

    # Load countries-regions dataset (required to get ISO codes).
    countries_regions = cast(catalog.Dataset, paths.load_dependency("regions"))["regions"]

    # Population data will also be loaded (used only to add a population column, and not to create any other derived
    # variables). Historical regions will be added to the population.

    # Load gdp (used only to add gdp column, and no other derived variables).
    gdp = (
        pd.DataFrame(catalog.Dataset(GDP_DATASET_PATH)["maddison_gdp"])
        .reset_index()[["country", "year", "gdp"]]
        .dropna()
    )

    # Load mapping from variable names in the component dataset to the final variable name in the output dataset.
    variable_mapping = pd.read_csv(VARIABLE_MAPPING_FILE).set_index(["source_variable"])

    #
    # Process data.
    #
    # Combine all tables.
    tables = {
        "energy_mix": tb_energy_mix.drop(columns=["country_code"], errors="ignore"),
        "fossil_fuel_production": tb_fossil_fuels,
        "primary_energy_consumption": tb_primary_energy.drop(columns=["gdp", "population", "source"], errors="ignore"),
        "electricity_mix": tb_electricity_mix.drop(
            columns=["population", "primary_energy_consumption__twh"], errors="ignore"
        ),
    }
    tb_combined = combine_tables_data_and_metadata(
        tables=tables,
        countries_regions=countries_regions,
        gdp=gdp,
        variable_mapping=variable_mapping,
    )

    #
    # Save outputs.
    #
    ds_garden = catalog.Dataset.create_empty(dest_dir)
    # Gather metadata sources from all tables' original dataset sources.
    ds_garden.metadata.sources = gather_sources_from_tables(tables=list(tables.values()))
    # Get the rest of the metadata from the yaml file.
    ds_garden.metadata.update_from_yaml(METADATA_PATH)
    # Create dataset.
    ds_garden.save()

    # Add other metadata fields to table.
    tb_combined.metadata.short_name = DATASET_SHORT_NAME
    tb_combined.metadata.title = DATASET_TITLE
    tb_combined.metadata.dataset = ds_garden.metadata

    # Add combined tables to the new dataset.
    ds_garden.add(tb_combined)
