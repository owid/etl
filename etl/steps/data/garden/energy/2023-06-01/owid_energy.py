"""Garden step that combines various datasets related to energy and produces the OWID Energy dataset.

Datasets combined:
* Energy mix from BP.
* Fossil fuel production (BP & Shift).
* Primary energy consumption (BP & EIA).
* Electricity mix (BP & Ember).

Auxiliary datasets:
* Regions (OWID).
* Population (OWID based on various sources).
* GDP (GGDC Maddison).

"""

from typing import Dict, cast

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.meta import Source
from owid.datautils import dataframes
from shared import add_population, gather_sources_from_tables

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Path to file with mapping of variable names from one of the datasets to the final energy dataset.
VARIABLE_MAPPING_FILE = paths.directory / "owid_energy_variable_mapping.csv"


def combine_tables_data_and_metadata(
    tables: Dict[str, Table],
    population: Table,
    countries_regions: Table,
    gdp: pd.DataFrame,
    variable_mapping: pd.DataFrame,
) -> Table:
    """Combine data and metadata of a list of tables, map variable names and add variables metadata.

    Parameters
    ----------
    tables : dict
        Dictionary where the key is the short name of the table, and the value is the actual table, for all tables to be
        combined.
    population: Table
        Population data.
    countries_regions : Table
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
    tb_combined : Table
        Combined table with metadata.

    """
    # Merge all tables as a dataframe (without metadata).
    dfs = [pd.DataFrame(table) for table in tables.values()]
    df_combined = dataframes.multi_merge(dfs, on=["country", "year"], how="outer")

    # Add ISO codes for countries (regions that are not in countries-regions dataset will have nan iso_code).
    df_combined = pd.merge(df_combined, countries_regions, left_on="country", right_on="name", how="left")

    # Add population and gdp of countries (except for dataset-specific regions e.g. those ending in (BP) or (Shift)).
    df_combined = add_population(df=df_combined, population=population, warn_on_missing_countries=False)
    df_combined = pd.merge(df_combined, gdp, on=["country", "year"], how="left")

    # Check that there were no repetition in column names.
    error = "Repeated columns in combined data."
    assert len([column for column in set(df_combined.columns) if "_x" in column]) == 0, error

    # Create a table with combined data and no metadata.
    tb_combined = Table(df_combined, short_name="owid_energy")

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
        tb_combined[source_variable].metadata.sources = [Source(name=variable_metadata["source"])]

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

    return cast(Table, tb_combined)


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Read all required datasets.
    ds_energy_mix: Dataset = paths.load_dependency("energy_mix")
    ds_fossil_fuels: Dataset = paths.load_dependency("fossil_fuel_production")
    ds_primary_energy: Dataset = paths.load_dependency("primary_energy_consumption")
    ds_electricity_mix: Dataset = paths.load_dependency("electricity_mix")
    ds_population: Dataset = paths.load_dependency("population")
    ds_ggdc: Dataset = paths.load_dependency("ggdc_maddison")

    # Gather all required tables from all datasets.
    tb_energy_mix = ds_energy_mix["energy_mix"].reset_index()
    tb_fossil_fuels = ds_fossil_fuels["fossil_fuel_production"].reset_index()
    tb_primary_energy = ds_primary_energy["primary_energy_consumption"].reset_index()
    tb_electricity_mix = ds_electricity_mix["electricity_mix"].reset_index()
    tb_population = ds_population["population"].reset_index()
    tb_regions = cast(Dataset, paths.load_dependency("regions"))["regions"]
    tb_ggdc = ds_ggdc["maddison_gdp"].reset_index()[["country", "year", "gdp"]].dropna()

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
        population=tb_population,
        countries_regions=tb_regions,
        gdp=tb_ggdc,
        variable_mapping=variable_mapping,
    )

    #
    # Save outputs.
    #
    # Gather metadata sources from all tables' original dataset sources.
    ds_garden = Dataset.create_empty(dest_dir)
    ds_garden.metadata.sources = gather_sources_from_tables(tables=list(tables.values()))

    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_combined], default_metadata=ds_garden.metadata)
    ds_garden.save()
