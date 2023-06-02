"""Garden step for Ember's Yearly Electricity Data.

"""

import numpy as np
import pandas as pd
from owid import catalog
from owid.catalog import Dataset, Table
from shared import add_population, add_region_aggregates, correct_data_points
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Initialize log.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Aggregate regions to add, following OWID definitions.
# Regions and income groups to create by aggregating contributions from member countries.
# In the following dictionary, if nothing is stated, the region is supposed to be a default continent/income group.
# Otherwise, the dictionary can have "regions_included", "regions_excluded", "countries_included", and
# "countries_excluded". The aggregates will be calculated on the resulting countries.
REGIONS = {
    # Default continents.
    "Africa": {},
    "Asia": {},
    "Europe": {},
    "European Union (27)": {},
    "North America": {},
    "Oceania": {},
    "South America": {},
    # Ember already has data for "World".
    # "World": {},
    # Income groups.
    "Low-income countries": {},
    "Upper-middle-income countries": {},
    "Lower-middle-income countries": {},
    "High-income countries": {},
}

# Corrections to the output tables.
# They are all the same correction: Remove region aggregates for the latest year, given that many countries are not
# informed, which causes the aggregates to be unreliable
# (e.g. generation__total_generation__twh in Africa drops in 2022 because only a few countries are informed).
AFFECTED_YEAR = 2022
AMENDMENTS = {
    "Capacity": [
        (
            {
                "country": list(REGIONS),
                "year": [AFFECTED_YEAR],
            },
            {
                "Clean (GW)": pd.NA,
                "Fossil (GW)": pd.NA,
                "Gas and Other Fossil (GW)": pd.NA,
                "Hydro, Bioenergy and Other Renewables (GW)": pd.NA,
                "Renewables (GW)": pd.NA,
                "Wind and Solar (GW)": pd.NA,
                "Bioenergy (GW)": pd.NA,
                "Coal (GW)": pd.NA,
                "Gas (GW)": pd.NA,
                "Hydro (GW)": pd.NA,
                "Nuclear (GW)": pd.NA,
                "Other Fossil (GW)": pd.NA,
                "Other Renewables (GW)": pd.NA,
                "Solar (GW)": pd.NA,
                "Wind (GW)": pd.NA,
            },
        )
    ],
    "Electricity demand": [
        (
            {
                "country": list(REGIONS),
                "year": [AFFECTED_YEAR],
            },
            {
                "Demand (TWh)": pd.NA,
                "population": pd.NA,
                "Demand per capita (kWh)": pd.NA,
            },
        )
    ],
    "Electricity generation": [
        (
            {
                "country": list(REGIONS),
                "year": [AFFECTED_YEAR],
            },
            {
                "Clean (%)": pd.NA,
                "Fossil (%)": pd.NA,
                "Gas and Other Fossil (%)": pd.NA,
                "Hydro, Bioenergy and Other Renewables (%)": pd.NA,
                "Renewables (%)": pd.NA,
                "Wind and Solar (%)": pd.NA,
                "Clean (TWh)": pd.NA,
                "Fossil (TWh)": pd.NA,
                "Gas and Other Fossil (TWh)": pd.NA,
                "Hydro, Bioenergy and Other Renewables (TWh)": pd.NA,
                "Renewables (TWh)": pd.NA,
                "Wind and Solar (TWh)": pd.NA,
                "Bioenergy (%)": pd.NA,
                "Coal (%)": pd.NA,
                "Gas (%)": pd.NA,
                "Hydro (%)": pd.NA,
                "Nuclear (%)": pd.NA,
                "Other Fossil (%)": pd.NA,
                "Other Renewables (%)": pd.NA,
                "Solar (%)": pd.NA,
                "Wind (%)": pd.NA,
                "Bioenergy (TWh)": pd.NA,
                "Coal (TWh)": pd.NA,
                "Gas (TWh)": pd.NA,
                "Hydro (TWh)": pd.NA,
                "Nuclear (TWh)": pd.NA,
                "Other Fossil (TWh)": pd.NA,
                "Other Renewables (TWh)": pd.NA,
                "Solar (TWh)": pd.NA,
                "Wind (TWh)": pd.NA,
                "Total Generation (TWh)": pd.NA,
            },
        ),
    ],
    "Electricity imports": [
        (
            {
                "country": list(REGIONS),
                "year": [AFFECTED_YEAR],
            },
            {
                "Net Imports (TWh)": np.nan,
            },
        ),
    ],
    "Power sector emissions": [
        (
            {
                "country": list(REGIONS),
                "year": [AFFECTED_YEAR],
            },
            {
                "Clean (mtCO2)": pd.NA,
                "Fossil (mtCO2)": pd.NA,
                "Gas and Other Fossil (mtCO2)": pd.NA,
                "Hydro, Bioenergy and Other Renewables (mtCO2)": pd.NA,
                "Renewables (mtCO2)": pd.NA,
                "Wind and Solar (mtCO2)": pd.NA,
                "Bioenergy (mtCO2)": pd.NA,
                "Coal (mtCO2)": pd.NA,
                "Gas (mtCO2)": pd.NA,
                "Hydro (mtCO2)": pd.NA,
                "Nuclear (mtCO2)": pd.NA,
                "Other Fossil (mtCO2)": pd.NA,
                "Other Renewables (mtCO2)": pd.NA,
                "Solar (mtCO2)": pd.NA,
                "Wind (mtCO2)": pd.NA,
                "Total emissions (mtCO2)": pd.NA,
                "Total Generation (TWh)": pd.NA,
                "CO2 intensity (gCO2/kWh)": pd.NA,
            },
        ),
    ],
}

# Conversion factors.
# Terawatt-hours to kilowatt-hours.
TWH_TO_KWH = 1e9
# Megatonnes to grams.
MT_TO_G = 1e12

# Columns to use from Ember's yearly electricity data, and how to rename them.
COLUMNS_YEARLY_ELECTRICITY = {
    "area": "country",
    "year": "year",
    "variable": "variable",
    "value": "value",
    "unit": "unit",
    "category": "category",
    "subcategory": "subcategory",
}

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


def make_wide_table(tb: Table, category: str, tb_regions: Table, tb_income: Table) -> Table:
    """Convert data from long to wide format for a specific category.

    This is a common processing for all categories in the data.

    Parameters
    ----------
    tb : Table
        Data, after harmonizing country names.
    category : str
        Name of category (as defined above in CATEGORIES) to process.
    tb_regions : Table
        Countries-regions data.
    tb_income : Table
        Data on income group definitions.

    Returns
    -------
    table : Table
        Table in wide format.

    """
    # Select data for given category.
    _tb = tb[tb["category"] == category].copy()

    # Pivot dataframe to have a column for each variable.
    table = Table(_tb.pivot(index=["country", "year"], columns=["variable", "unit"], values="value"))

    # Get variable names, units, and variable-units (a name that combines both) for each column.
    variable_units = [f"{variable} ({unit})" for variable, unit in table.columns]

    # Sanity check.
    variables = table.columns.get_level_values(0).tolist()
    units = table.columns.get_level_values(1).tolist()
    assert len(variable_units) == len(units) == len(variables)

    # Collapse the two column levels into one, with the naming "variable (unit)" (except for country and year, that
    # have no units and are the indexes of the table).
    table.columns = variable_units

    # Add region aggregates.
    aggregates = {column: "sum" for column in SUM_AGGREGATES if column in table.columns}

    table = add_region_aggregates(
        data=table.reset_index(),
        index_columns=["country", "year"],
        regions_to_add=REGIONS,
        aggregates=aggregates,
        df_regions=tb_regions,
        df_income=tb_income.rename(columns={"classification": "income_group"}),
    )

    return table


def make_table_electricity_generation(tb: Table, tb_regions: Table, tb_income: Table) -> Table:
    """Create table with processed data of category "Electricity generation".

    Parameters
    ----------
    tb : Table
        Data in long format for all categories, after harmonizing country names.
    tb_regions : Table
        Countries-regions data.
    tb_income : Table
        Data on income group definitions.

    Returns
    -------
    table : Table
        Table of processed data for the given category.

    """
    # Prepare wide table.
    table = make_wide_table(tb=tb, category="Electricity generation", tb_regions=tb_regions, tb_income=tb_income)

    # Recalculate the share of electricity generates for region aggregates.
    for column in table.columns:
        if "(%)" in column:
            # Find corresponding column with units instead of percentages.
            value_column = column.replace("(%)", "(TWh)")
            if value_column not in table.columns:
                raise ValueError(f"Column {value_column} not found.")
            # Select only regions.
            select_regions = table["country"].isin(list(REGIONS))
            table.loc[select_regions, column] = table[value_column] / table["Total Generation (TWh)"] * 100

    return table


def make_table_electricity_demand(tb: Table, population: Table, tb_regions: Table, tb_income: Table) -> Table:
    """Create table with processed data of category "Electricity demand".

    Parameters
    ----------
    tb : Table
        Data in long format for all categories, after harmonizing country names.
    tb_regions : Table
        Countries-regions data.
    tb_income : Table
        Data on income group definitions.

    Returns
    -------
    table : Table
        Table of processed data for the given category.

    """
    # Prepare wide table.
    table = make_wide_table(tb=tb, category="Electricity demand", tb_regions=tb_regions, tb_income=tb_income)

    # Add population to data
    table = add_population(df=table, population=population, warn_on_missing_countries=False)

    # Recalculate demand per capita.
    # We could do this only for region aggregates (since they do not have per capita values),
    # but we do this for all countries, to ensure per-capita variables are consistent with our population data.
    table["Demand per capita (kWh)"] = (
        pd.DataFrame(table)["Demand (TWh)"] * TWH_TO_KWH / pd.DataFrame(table)["population"]
    )

    # Delete the original demand per capita column.
    table = table.drop(columns=["Demand per capita (MWh)"])

    return table


def make_table_power_sector_emissions(tb: Table, tb_regions: Table, tb_income: Table) -> Table:
    """Create table with processed data of category "Power sector emissions".

    Parameters
    ----------
    tb : Table
        Data in long format for all categories, after harmonizing country names.
    tb_regions : Table
        Countries-regions data.
    tb_income : Table
        Data on income group definitions.

    Returns
    -------
    table : Table
        Table of processed data for the given category.

    """
    # Prepare wide table of emissions data.
    table = make_wide_table(tb=tb, category="Power sector emissions", tb_regions=tb_regions, tb_income=tb_income)

    # Add carbon intensity.
    # In principle this only needs to be done for region aggregates, but we do it for all countries and check that
    # the results are consistent with the original data.
    # Prepare wide table also for electricity generation (required to calculate carbon intensity).
    electricity = make_wide_table(tb=tb, category="Electricity generation", tb_regions=tb_regions, tb_income=tb_income)[
        ["country", "year", "Total Generation (TWh)"]
    ]
    # Add total electricity generation to emissions table.
    table = pd.merge(table, electricity, on=["country", "year"], how="left")
    # Rename the original carbon intensity column as a temporary column called "check".
    intensity_col = "CO2 intensity (gCO2/kWh)"
    table = table.rename(columns={intensity_col: "check"})
    # Calculate carbon intensity for all countries and regions.
    table[intensity_col] = (
        pd.DataFrame(table)["Total emissions (mtCO2)"] * MT_TO_G / (table["Total Generation (TWh)"] * TWH_TO_KWH)
    )

    # Check that the new carbon intensities agree (within 1 % of mean average percentage error, aka mape) with the
    # original ones (where carbon intensity was given, namely for countries, not aggregate regions).
    mape = 100 * abs(table.dropna(subset="check")[intensity_col] - table["check"].dropna()) / table["check"].dropna()
    assert mape.max() < 1, "Calculated carbon intensities differ from original ones by more than 1 percent."

    # Remove temporary column.
    table = table.drop(columns=["check"])

    return table


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load dataset from meadow and read its main table.
    ds_meadow: Dataset = paths.load_dependency("yearly_electricity")
    tb_meadow = ds_meadow["yearly_electricity"].reset_index()

    # Load population dataset and read its main table.
    ds_population: Dataset = paths.load_dependency("population")
    tb_population = ds_population["population"]

    # Load regions dataset and read its main table.
    ds_regions: Dataset = paths.load_dependency("regions")
    tb_regions = ds_regions["regions"]

    # Load income groups dataset and read its main table.
    ds_income: Dataset = paths.load_dependency("income_groups")
    tb_income = ds_income["income_groups_latest"].reset_index()

    #
    # Process data.
    #
    # Select and rename columns conveniently.
    tb = tb_meadow[list(COLUMNS_YEARLY_ELECTRICITY)].rename(columns=COLUMNS_YEARLY_ELECTRICITY, errors="raise")

    # Sanity check.
    assert set(tb["category"]) == set(CATEGORIES), "Categories have changed in data."

    # Harmonize country names.
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, warn_on_missing_countries=True, warn_on_unused_countries=True
    )

    # Split data into different tables, one per category, and process each one individually.
    tables = {
        "Capacity": make_wide_table(tb=tb, category="Capacity", tb_regions=tb_regions, tb_income=tb_income),
        "Electricity demand": make_table_electricity_demand(
            tb=tb, population=tb_population, tb_regions=tb_regions, tb_income=tb_income
        ),
        "Electricity generation": make_table_electricity_generation(tb=tb, tb_regions=tb_regions, tb_income=tb_income),
        "Electricity imports": make_wide_table(
            tb=tb, category="Electricity imports", tb_regions=tb_regions, tb_income=tb_income
        ),
        "Power sector emissions": make_table_power_sector_emissions(tb=tb, tb_regions=tb_regions, tb_income=tb_income),
    }

    # Apply amendments, and set an appropriate index and short name to each table an sort conveniently.
    for table_name in tables:
        if table_name in AMENDMENTS:
            log.info(f"Applying amendments to table: {table_name}")
            tables[table_name] = correct_data_points(df=tables[table_name], corrections=AMENDMENTS[table_name])
        tables[table_name] = tables[table_name].set_index(["country", "year"], verify_integrity=True).sort_index()
        tables[table_name].metadata.short_name = catalog.utils.underscore(table_name)

    #
    # Save outputs.
    #
    # Create a new dataset with the same metadata as in Meadow.
    ds_garden = create_dataset(dest_dir, tables=list(tables.values()), default_metadata=ds_meadow.metadata)
    ds_garden.save()
