"""Garden step for Global Electricity Review (Ember, 2022).

"""

import pandas as pd
from owid import catalog
from owid.datautils import geo

from etl.paths import DATA_DIR
from shared import (
    CURRENT_DIR,
    OVERLAPPING_DATA_TO_REMOVE_IN_AGGREGATES,
    REGIONS_TO_ADD,
    add_population,
    add_region_aggregates,
    log,
)

# Details for dataset to export.
DATASET_SHORT_NAME = "global_electricity_review"
# Details for dataset to import.
MEADOW_DATASET_PATH = DATA_DIR / f"meadow/ember/2022-08-01/{DATASET_SHORT_NAME}"

COUNTRY_MAPPING_PATH = CURRENT_DIR / f"{DATASET_SHORT_NAME}.countries.json"
METADATA_PATH = CURRENT_DIR / f"{DATASET_SHORT_NAME}.meta.yml"

# Conversion factors.
# Terawatt-hours to kilowatt-hours.
TWH_TO_KWH = 1e9
# Megatonnes to grams.
MT_TO_G = 1e12

# Map units (short version) to unit name (long version).
SHORT_UNIT_TO_UNIT = {
    "TWh": "terawatt-hours",
    "MWh": "megawatt-hours",
    "kWh": "kilowatt-hours",
    "mtCO2": "megatonnes of CO2 equivalent",
    "gCO2/kWh": "grams of CO2 equivalent per kilowatt-hour",
    "GW": "gigawatts",
    "%": "%",
}

# Categories expected to exist in the data.
CATEGORIES = [
    "Capacity",
    "Electricity demand",
    "Electricity generation",
    "Electricity imports",
    "Power sector emissions",
]

# Choose columns for which region aggregates should be created.
SUM_AGGREGATES = [
    # "Bioenergy (%)",
    "Bioenergy (GW)",
    "Bioenergy (TWh)",
    "Bioenergy (mtCO2)",
    # "CO2 intensity (gCO2/kWh)",
    # "Clean (%)",
    "Clean (GW)",
    "Clean (TWh)",
    "Clean (mtCO2)",
    # "Coal (%)",
    "Coal (GW)",
    "Coal (TWh)",
    "Coal (mtCO2)",
    "Demand (TWh)",
    # "Demand per capita (MWh)",
    # "Fossil (%)",
    "Fossil (GW)",
    "Fossil (TWh)",
    "Fossil (mtCO2)",
    # "Gas (%)",
    "Gas (GW)",
    "Gas (TWh)",
    "Gas (mtCO2)",
    "Gas and Other Fossil (%)",
    "Gas and Other Fossil (GW)",
    "Gas and Other Fossil (TWh)",
    "Gas and Other Fossil (mtCO2)",
    # "Hydro (%)",
    "Hydro (GW)",
    "Hydro (TWh)",
    "Hydro (mtCO2)",
    "Hydro, Bioenergy and Other Renewables (%)",
    "Hydro, Bioenergy and Other Renewables (GW)",
    "Hydro, Bioenergy and Other Renewables (TWh)",
    "Hydro, Bioenergy and Other Renewables (mtCO2)",
    "Net Imports (TWh)",
    # "Nuclear (%)",
    "Nuclear (GW)",
    "Nuclear (TWh)",
    "Nuclear (mtCO2)",
    # "Other Fossil (%)",
    "Other Fossil (GW)",
    "Other Fossil (TWh)",
    "Other Fossil (mtCO2)",
    # "Other Renewables (%)",
    "Other Renewables (GW)",
    "Other Renewables (TWh)",
    "Other Renewables (mtCO2)",
    # "Renewables (%)",
    "Renewables (GW)",
    "Renewables (TWh)",
    "Renewables (mtCO2)",
    # "Solar (%)",
    "Solar (GW)",
    "Solar (TWh)",
    "Solar (mtCO2)",
    "Total Generation (TWh)",
    "Total emissions (mtCO2)",
    # "Wind (%)",
    "Wind (GW)",
    "Wind (TWh)",
    "Wind (mtCO2)",
    # "Wind and Solar (%)",
    "Wind and Solar (GW)",
    "Wind and Solar (TWh)",
    "Wind and Solar (mtCO2)",
]


def prepare_wide_table(df: pd.DataFrame, category: str) -> catalog.Table:
    # Common processing for all categories in the data.

    # Select data for given category.
    _df = df[df["category"] == category].copy()

    # Pivot dataframe to have a column for each variable.
    table = catalog.Table(
        _df.pivot(
            index=["country", "year"], columns=["variable", "unit"], values="value"
        )
    )

    # Get variable names, units, and variable-units (a name that combines both) for each column.
    variables = table.columns.get_level_values(0).tolist()
    units = table.columns.get_level_values(1).tolist()
    variable_units = [f"{variable} ({unit})" for variable, unit in table.columns]

    assert len(variable_units) == len(units) == len(variables)

    # Collapse the two column levels into one, with the naming "variable (unit)" (except for country and year, that
    # have no units).
    table.columns = variable_units

    # Add region aggregates.
    aggregates = {column: "sum" for column in SUM_AGGREGATES if column in table.columns}
    table = add_region_aggregates(
        data=table.reset_index(),
        regions=list(REGIONS_TO_ADD),
        index_columns=["country", "year"],
        aggregates=aggregates,
        known_overlaps=OVERLAPPING_DATA_TO_REMOVE_IN_AGGREGATES,
        keep_original_region_with_suffix=" (Ember)",
    )

    # Add variable names and units to the metadata.
    for column, variable, unit in zip(variable_units, variables, units):
        table[column].metadata.title = column
        table[column].metadata.short_unit = unit
        table[column].metadata.unit = SHORT_UNIT_TO_UNIT[unit]
        table[column].metadata.display = {"name": variable}

    return table


def process_capacity_data(df: pd.DataFrame) -> catalog.Table:
    # Prepare wide table.
    table = prepare_wide_table(df=df, category="Capacity")

    return table


def process_electricity_generation(df: pd.DataFrame) -> catalog.Table:
    # Prepare wide table.
    table = prepare_wide_table(df=df, category="Electricity generation")

    # Recalculate the share of electricity generates for region aggregates.
    for column in table.columns:
        if "(%)" in column:
            # Find corresponding column with units instead of percentages.
            value_column = column.replace("(%)", "(TWh)")
            if value_column not in table.columns:
                raise ValueError(f"Column {value_column} not found.")
            # Select only regions.
            select_regions = table["country"].isin(list(REGIONS_TO_ADD))
            table.loc[select_regions, column] = (
                table[value_column] / table["Total Generation (TWh)"] * 100
            )

    return table


def process_electricity_demand(df: pd.DataFrame) -> catalog.Table:
    # Prepare wide table.
    table = prepare_wide_table(df=df, category="Electricity demand")

    # Store variable metadata in a dictionary, since it will be lost when processing data.
    table_metadata = {column: table[column].metadata for column in table.columns}

    # Include variable metadata for additional variables that will be created.
    table_metadata["population"] = catalog.VariableMeta(
        title="Population", unit="people", short_unit="people"
    )
    table_metadata["Demand per capita (kWh)"] = catalog.VariableMeta(
        title="Demand per capita (kWh)",
        unit="kilowatt-hours",
        short_unit="kWh",
        display={"name": "Demand per capita"},
    )

    # Add population to data
    table = add_population(df=table, warn_on_missing_countries=False)

    # Recalculate demand per capita.
    # We could do this only for region aggregates (since they do not have per capita values),
    # but we do this for all countries, to ensure per-capita variables are consistent with our population data.
    table["Demand per capita (kWh)"] = (
        pd.DataFrame(table)["Demand (TWh)"]
        * TWH_TO_KWH
        / pd.DataFrame(table)["population"]
    )

    # Restore original metadata for each column (and add the new ones).
    for column in table.columns:
        table[column].metadata = table_metadata[column]

    # Delete the original demand per capita column.
    table = table.drop(columns="Demand per capita (MWh)")

    return table


def process_electricity_imports(df: pd.DataFrame) -> catalog.Table:
    # Prepare wide table.
    table = prepare_wide_table(df=df, category="Electricity imports")

    return table


def process_power_sector_emissions(df: pd.DataFrame) -> catalog.Table:
    # Prepare wide table of emissions data.
    table = prepare_wide_table(df=df, category="Power sector emissions")

    # Store variable metadata in a dictionary, since it will be lost when processing data.
    table_metadata = {column: table[column].metadata for column in table.columns}

    # Include variable metadata for additional variables that will be created.
    table_metadata["Total Generation (TWh)"] = catalog.VariableMeta(
        title="Total Generation (TWh)",
        unit="terawatt-hours",
        short_unit="TWh",
        display={"name": "Total Generation"},
    )

    # Add carbon intensity.
    # In principle this only needs to be done for region aggregates, but we do it for all countries and check that
    # the results are consistent with the original data.
    # Prepare wide table also for electricity generation (required to calculate carbon intensity).
    electricity = prepare_wide_table(df=df, category="Electricity generation")[
        ["country", "year", "Total Generation (TWh)"]
    ]
    # Add total electricity generation to emissions table.
    table = pd.merge(table, electricity, on=["country", "year"], how="left")
    # Rename the original carbon intensity column as a temporary column called "check".
    intensity_col = "CO2 intensity (gCO2/kWh)"
    table = table.rename(columns={intensity_col: "check"})
    # Calculate carbon intensity for all countries and regions.
    table[intensity_col] = (
        pd.DataFrame(table)["Total emissions (mtCO2)"]
        * MT_TO_G
        / (table["Total Generation (TWh)"] * TWH_TO_KWH)
    )
    # Check that the new carbon intensities agree (within 1 % of mean average percentage error) with the original
    # ones (where carbon intensity was given, namely for countries, not aggregate regions).
    mape = (
        100
        * abs(table.dropna(subset="check")[intensity_col] - table["check"].dropna())
        / table["check"].dropna()
    )
    assert (
        mape.max() < 1
    ), "Calculated carbon intensities differ from original ones by more than 1 percent."
    # Remove temporary column.
    table = table.drop(columns="check")

    # Restore original metadata for each column (and add the new ones).
    for column in table.columns:
        table[column].metadata = table_metadata[column]

    return table


def run(dest_dir: str) -> None:
    log.info(f"{DATASET_SHORT_NAME}.start")

    #
    # Load data.
    #
    # Read dataset from meadow.
    ds_meadow = catalog.Dataset(MEADOW_DATASET_PATH)
    # Get table from dataset.
    tb_meadow = ds_meadow[DATASET_SHORT_NAME]
    # Make a dataframe out of the data in the table.
    raw = pd.DataFrame(tb_meadow)

    #
    # Process data.
    #
    # Select and rename columns conveniently.
    columns = {
        "area": "country",
        "year": "year",
        "variable": "variable",
        "value": "value",
        "unit": "unit",
        "category": "category",
        "subcategory": "subcategory",
    }
    df = raw.reset_index()[list(columns)].rename(columns=columns)

    # Sanity check.
    assert set(df["category"]) == set(CATEGORIES), "Categories have changed in data."

    # Harmonize country names.
    log.info(f"{DATASET_SHORT_NAME}.harmonize_countries")
    df = geo.harmonize_countries(df=df, countries_file=str(COUNTRY_MAPPING_PATH))

    # Split data into different tables, one per category, and process each one individually.
    log.info(f"{DATASET_SHORT_NAME}.prepare_wide_tables")
    tables = {
        "Capacity": process_capacity_data(df=df),
        "Electricity demand": process_electricity_demand(df=df),
        "Electricity generation": process_electricity_generation(df=df),
        "Electricity imports": process_electricity_imports(df=df),
        "Power sector emissions": process_power_sector_emissions(df=df),
    }

    #
    # Save outputs.
    #
    ds_garden = catalog.Dataset.create_empty(dest_dir)
    ds_garden.metadata = ds_meadow.metadata
    ds_garden.metadata.update_from_yaml(METADATA_PATH)
    ds_garden.save()

    # Add all tables to dataset.
    for table_name in list(tables):
        table = tables[table_name]
        table = table.set_index(["country", "year"], verify_integrity=True).sort_index()
        table = catalog.utils.underscore_table(table)
        table.metadata = tb_meadow.metadata
        # table.update_metadata_from_yaml(METADATA_PATH, DATASET_SHORT_NAME)
        table.metadata.title = table_name
        table.metadata.short_name = catalog.utils.underscore(table_name)
        ds_garden.add(table)

    log.info(f"{DATASET_SHORT_NAME}.end")
