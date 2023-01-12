from typing import Dict, List

import pandas as pd
from owid import catalog
from owid.datautils import dataframes
from shared import CURRENT_DIR

from etl.data_helpers import geo
from etl.paths import DATA_DIR

# Details for dataset to export.
DATASET_SHORT_NAME = "ghg_emissions_by_sector"
COUNTRY_MAPPING_PATH = CURRENT_DIR / f"{DATASET_SHORT_NAME}.countries.json"
METADATA_PATH = CURRENT_DIR / f"{DATASET_SHORT_NAME}.meta.yml"
# Details for dataset to import.
MEADOW_DATASET_PATH = DATA_DIR / f"meadow/cait/2022-08-10/{DATASET_SHORT_NAME}"

# All sectors expected in the data, and how to rename them.
SECTORS = {
    "Agriculture": "Agriculture",
    "Building": "Buildings",
    "Bunker Fuels": "Aviation and shipping",
    "Electricity/Heat": "Electricity and heat",
    "Energy": "Energy",
    "Fugitive Emissions": "Fugitive emissions",
    "Industrial Processes": "Industry",
    "Land-Use Change and Forestry": "Land-use change and forestry",
    "Manufacturing/Construction": "Manufacturing and construction",
    "Other Fuel Combustion": "Other fuel combustion",
    "Total excluding LUCF": "Total excluding LUCF",
    "Total including LUCF": "Total including LUCF",
    "Transportation": "Transport",
    "Waste": "Waste",
}

# Suffix to add to the name of per capita variables.
PER_CAPITA_SUFFIX = " (per capita)"

# Mapping of gas name (as given in CAIT data) to the name of the corresponding output table.
TABLE_NAMES = {
    "All GHG": "Greenhouse gas emissions by sector",
    "CH4": "Methane emissions by sector",
    "CO2": "Carbon dioxide emissions by sector",
    "F-Gas": "Fluorinated gas emissions by sector",
    "N2O": "Nitrous oxide emissions by sector",
}

# Aggregate regions to add, following OWID definitions.
REGIONS_TO_ADD = [
    # Continents.
    "Africa",
    "Asia",
    "Europe",
    # The EU27 is already included in the original data, and after inspection the data coincides with our aggregate.
    # So we simply keep the original data for EU27 given in the data.
    "North America",
    "Oceania",
    "South America",
    # Income groups.
    "Low-income countries",
    "Upper-middle-income countries",
    "Lower-middle-income countries",
    "High-income countries",
]

# Convert million tonnes to tonnes.
MT_TO_T = 1e6


def create_table_for_gas(df: pd.DataFrame, gas: str, countries_in_regions: Dict[str, List[str]]) -> catalog.Table:
    """Extract data for a particular gas and create a table with variables' metadata.

    Parameters
    ----------
    df : pd.DataFrame
    gas : str
        Name of gas to consider (as called in "gas" column of the original data).
    countries_in_regions : dict
        Countries in regions (a dictionary where each key is the name of the region, and the value is a list of country
        names in that region). This is used to avoid loading the list of countries in a region for each gas.

    Returns
    -------
    table_gas : catalog.Table
        Table with data for considered gas, and metadata for each variable.

    """
    # Select data for current gas.
    df_gas = df[df["gas"] == gas].drop(columns="gas").reset_index(drop=True)

    # Pivot table to have a column for each sector.
    df_gas = df_gas.pivot(index=["country", "year"], columns="sector", values="value").reset_index()

    # Create region aggregates.
    for region in REGIONS_TO_ADD:
        df_gas = geo.add_region_aggregates(
            df=df_gas,
            region=region,
            countries_in_region=countries_in_regions[region],
            countries_that_must_have_data=[],
            num_allowed_nans_per_year=None,
            frac_allowed_nans_per_year=0.2,
            aggregations=None,
            keep_original_region_with_suffix=" (CAIT)",
        )

    # Add population to data.
    df_gas = geo.add_population_to_dataframe(df=df_gas)

    # Add per capita variables.
    variables = [column for column in df_gas.columns if column not in ["country", "year", "population"]]
    for variable in variables:
        new_column = variable + PER_CAPITA_SUFFIX
        df_gas[new_column] = MT_TO_T * df_gas[variable] / df_gas["population"]

    # Remove columns that only have nans.
    df_gas = df_gas.drop(columns=df_gas.columns[df_gas.isnull().all()])
    # Remove rows that only have nans.
    df_gas = df_gas.dropna(
        subset=[column for column in df_gas.columns if column not in ["country", "year"]],
        how="all",
    ).reset_index(drop=True)

    # Set index and sort rows and columns conveniently.
    df_gas = df_gas.set_index(["country", "year"], verify_integrity=True).sort_index()
    df_gas = df_gas[sorted(df_gas.columns)]

    # Create table with this data but no metadata.
    table_gas = catalog.Table(df_gas)
    # Create variable metadata.
    for variable in table_gas.columns:
        if PER_CAPITA_SUFFIX in variable:
            table_gas[variable].metadata.unit = "tonnes per capita"
            table_gas[variable].metadata.short_unit = "t"
            table_gas[variable].metadata.title = variable
            table_gas[variable].metadata.display = {"name": variable.replace(PER_CAPITA_SUFFIX, "")}
        else:
            table_gas[variable].metadata.unit = "million tonnes"
            table_gas[variable].metadata.short_unit = "million t"
            table_gas[variable].metadata.title = variable
            table_gas[variable].metadata.display = {
                "name": variable,
                "numDecimalPlaces": 0,
            }

    return table_gas


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Read dataset from meadow.
    ds_meadow = catalog.Dataset(MEADOW_DATASET_PATH)
    # Get table from meadow dataset.
    tb_meadow = ds_meadow[ds_meadow.table_names[0]]
    # Get dataframe from table.
    df = pd.DataFrame(tb_meadow).reset_index()

    # List all countries inside each region.
    countries_in_regions = {
        region: sorted(set(geo.list_countries_in_region(region)) & set(df["country"])) for region in REGIONS_TO_ADD
    }

    #
    # Process data.
    #
    # Select only one data source (CAIT).
    df = df[df["data_source"] == "CAIT"].reset_index(drop=True)

    # Check that there is only one unit in dataset.
    assert set(df["unit"]) == {"MtCO₂e"}, "Unknown units in dataset"
    # Remove unnecessary columns.
    df = df.drop(columns=["unit", "id", "data_source", "iso_code3"])

    # Rename sectors.
    df["sector"] = dataframes.map_series(
        series=df["sector"],
        mapping=SECTORS,
        warn_on_missing_mappings=True,
        warn_on_unused_mappings=True,
    )

    # Harmonize country names.
    df = geo.harmonize_countries(
        df=df,
        countries_file=COUNTRY_MAPPING_PATH,
        warn_on_missing_countries=True,
        warn_on_unused_countries=True,
    )

    # Create one table for each gas, and one for all gases combined.
    tables = {
        gas: create_table_for_gas(df=df, gas=gas, countries_in_regions=countries_in_regions)
        for gas in df["gas"].unique()
    }

    #
    # Save outputs.
    #
    ds_garden = catalog.Dataset.create_empty(dest_dir)
    # Import metadata from meadow dataset and update attributes using the metadata yaml file.
    ds_garden.metadata.update_from_yaml(METADATA_PATH)
    # Create dataset.
    ds_garden.save()

    # Add all tables to dataset.
    for table_name in list(tables):
        table_title = TABLE_NAMES[table_name]
        table_short_name = catalog.utils.underscore(table_title)
        table = tables[table_name]
        # Make column names snake lower case.
        table = catalog.utils.underscore_table(table)
        table.metadata.title = table_title
        table.metadata.short_name = table_short_name
        table.update_metadata_from_yaml(METADATA_PATH, table_short_name)
        # Add table to dataset.
        ds_garden.add(table)
