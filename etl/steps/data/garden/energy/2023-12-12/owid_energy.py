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

from typing import Dict, cast

import numpy as np
import owid.catalog.processing as pr
from owid.catalog import Dataset, Table
from owid.catalog.meta import Source
from owid.catalog.tables import (
    get_unique_licenses_from_tables,
    get_unique_sources_from_tables,
)

from etl.data_helpers.geo import add_population_to_table
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Path to file with mapping of variable names from one of the datasets to the final energy dataset.
VARIABLE_MAPPING_FILE = paths.directory / "owid_energy_variable_mapping.csv"


def combine_tables_data_and_metadata(
    tables: Dict[str, Table],
    ds_population: Dataset,
    tb_regions: Table,
    tb_gdp: Table,
    variable_mapping: Table,
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
    variable_mapping : Table
        Table (with columns variable, source_variable, source_dataset, description, source) that specifies the names
        of variables to take from each table, and their new name in the output table. It also gives a description of the
        variable, and the sources of the table.

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
    tb_combined = pr.merge(tb_combined, tb_regions, left_on="country", right_on="name", how="left")

    # Add population and gdp of countries (except for dataset-specific regions e.g. those ending in (BP) or (Shift)).
    tb_combined = add_population_to_table(tb=tb_combined, ds_population=ds_population, warn_on_missing_countries=False)
    tb_combined = pr.merge(tb_combined, tb_gdp, on=["country", "year"], how="left")

    # Check that there were no repetition in column names.
    error = "Repeated columns in combined data."
    assert len([column for column in set(tb_combined.columns) if "_x" in column]) == 0, error

    # List the names of the variables described in the variable mapping file.
    source_variables = variable_mapping.index.get_level_values(0).tolist()

    # Gather original metadata for each variable, add the descriptions and sources from the variable mapping file.
    for source_variable in source_variables:
        variable_metadata = variable_mapping.loc[source_variable]
        source_dataset = variable_metadata["source_dataset"]
        # Check that the variable indeed exists in the original dataset that the variable mapping says.
        # Ignore columns "country", "year" (assigned to a dummy dataset "various_datasets"), "population" (that comes
        # from the "population" dataset) and "iso_alpha3" (that comes from the "regions" dataset).
        if source_dataset not in [
            "various_datasets",
            "regions",
            "population",
            "maddison_gdp",
        ]:
            error = f"Variable {source_variable} not found in any of the original datasets."
            assert source_variable in tables[source_dataset].columns, error
            tb_combined[source_variable].metadata = tables[source_dataset][source_variable].metadata

        # Update metadata with the content of the variable mapping file.
        tb_combined[source_variable].metadata.description = variable_metadata["description"]
        tb_combined[source_variable].metadata.sources = [Source(name=variable_metadata["source"])]

    # Select only variables in the mapping file, and rename variables according to the mapping.
    tb_combined = tb_combined[source_variables].rename(columns=variable_mapping.to_dict()["variable"], errors="raise")

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
    ds_gdp: Dataset = paths.load_dependency("ggdc_maddison")
    ds_regions: Dataset = paths.load_dependency("regions")

    # Gather all required tables from all datasets.
    tb_energy_mix = ds_energy_mix["energy_mix"].reset_index()
    tb_fossil_fuels = ds_fossil_fuels["fossil_fuel_production"].reset_index()
    tb_primary_energy = ds_primary_energy["primary_energy_consumption"].reset_index()
    tb_electricity_mix = ds_electricity_mix["electricity_mix"].reset_index()
    tb_regions = ds_regions["regions"].reset_index()[["name", "iso_alpha3"]].dropna().reset_index(drop=True)
    tb_gdp = ds_gdp["maddison_gdp"].reset_index()[["country", "year", "gdp"]].dropna().reset_index(drop=True)

    # Load mapping from variable names in the component dataset to the final variable name in the output dataset.
    variable_mapping = pr.read_csv(VARIABLE_MAPPING_FILE).set_index(["source_variable"])

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
        tb_regions=tb_regions,
        tb_gdp=tb_gdp,
        variable_mapping=variable_mapping,
    )

    #
    # Save outputs.
    #
    # Gather metadata sources from all tables' original dataset sources.
    ds_garden = Dataset.create_empty(dest_dir)
    ds_garden.metadata.sources = get_unique_sources_from_tables(tables=tables.values())
    ds_garden.metadata.licenses = get_unique_licenses_from_tables(tables=tables.values())

    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_combined], default_metadata=ds_garden.metadata)
    ds_garden.save()
