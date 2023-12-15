"""Garden step for Ember's Yearly Electricity Data.

"""

import owid.catalog.processing as pr
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Initialize log.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

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
    # "Bioenergy - %",
    "Bioenergy - GW",
    "Bioenergy - TWh",
    "Bioenergy - mtCO2",
    # "CO2 intensity - gCO2/kWh",
    # "Clean - %",
    "Clean - GW",
    "Clean - TWh",
    "Clean - mtCO2",
    # "Coal - %",
    "Coal - GW",
    "Coal - TWh",
    "Coal - mtCO2",
    "Demand - TWh",
    # "Demand per capita - MWh",
    # "Fossil - %",
    "Fossil - GW",
    "Fossil - TWh",
    "Fossil - mtCO2",
    # "Gas - %",
    "Gas - GW",
    "Gas - TWh",
    "Gas - mtCO2",
    "Gas and Other Fossil - %",
    "Gas and Other Fossil - GW",
    "Gas and Other Fossil - TWh",
    "Gas and Other Fossil - mtCO2",
    # "Hydro - %",
    "Hydro - GW",
    "Hydro - TWh",
    "Hydro - mtCO2",
    "Hydro, Bioenergy and Other Renewables - %",
    "Hydro, Bioenergy and Other Renewables - GW",
    "Hydro, Bioenergy and Other Renewables - TWh",
    "Hydro, Bioenergy and Other Renewables - mtCO2",
    "Net Imports - TWh",
    # "Nuclear - %",
    "Nuclear - GW",
    "Nuclear - TWh",
    "Nuclear - mtCO2",
    # "Other Fossil - %",
    "Other Fossil - GW",
    "Other Fossil - TWh",
    "Other Fossil - mtCO2",
    # "Other Renewables - %",
    "Other Renewables - GW",
    "Other Renewables - TWh",
    "Other Renewables - mtCO2",
    # "Renewables - %",
    "Renewables - GW",
    "Renewables - TWh",
    "Renewables - mtCO2",
    # "Solar - %",
    "Solar - GW",
    "Solar - TWh",
    "Solar - mtCO2",
    "Total Generation - TWh",
    "Total emissions - mtCO2",
    # "Wind - %",
    "Wind - GW",
    "Wind - TWh",
    "Wind - mtCO2",
    # "Wind and Solar - %",
    "Wind and Solar - GW",
    "Wind and Solar - TWh",
    "Wind and Solar - mtCO2",
]


def make_wide_table(tb: Table, category: str, ds_regions: Dataset, ds_income_groups: Dataset) -> Table:
    """Convert data from long to wide format for a specific category.

    This is a common processing for all categories in the data.

    Parameters
    ----------
    tb : Table
        Data, after harmonizing country names.
    category : str
        Name of category (as defined above in CATEGORIES) to process.
    ds_regions : Dataset
        Regions dataset.
    ds_income : Dataset
        Income groups dataset.

    Returns
    -------
    table : Table
        Table in wide format.

    """
    # Select data for given category.
    _tb = tb[tb["category"] == category].copy()

    # Pivot table to have a column for each variable.
    table = _tb.pivot(
        index=["country", "year"], columns=["variable", "unit"], values="value", join_column_levels_with=" - "
    )

    # Add region aggregates.
    aggregates = {column: "sum" for column in SUM_AGGREGATES if column in table.columns}
    table = geo.add_regions_to_table(
        table,
        aggregations=aggregates,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        ignore_overlaps_of_zeros=True,
    )

    return table


def make_table_electricity_generation(tb: Table, ds_regions: Dataset, ds_income_groups: Dataset) -> Table:
    """Create table with processed data of category "Electricity generation".

    Parameters
    ----------
    tb : Table
        Data in long format for all categories, after harmonizing country names.
    ds_regions : Dataset
        Regions dataset.
    ds_income_groups : Dataset
        Income groups dataset.

    Returns
    -------
    table : Table
        Table of processed data for the given category.

    """
    # Prepare wide table.
    table = make_wide_table(
        tb=tb, category="Electricity generation", ds_regions=ds_regions, ds_income_groups=ds_income_groups
    )

    # Recalculate the share of electricity generates for region aggregates.
    for column in table.columns:
        if "%" in column:
            # Find corresponding column with units instead of percentages.
            value_column = column.replace("%", "TWh")
            if value_column not in table.columns:
                raise ValueError(f"Column {value_column} not found.")
            # Select only regions.
            select_regions = table["country"].isin(list(geo.REGIONS))
            table.loc[select_regions, column] = table[value_column] / table["Total Generation - TWh"] * 100

    return table


def make_table_electricity_demand(
    tb: Table, ds_population: Dataset, ds_regions: Dataset, ds_income_groups: Dataset
) -> Table:
    """Create table with processed data of category "Electricity demand".

    Parameters
    ----------
    tb : Table
        Data in long format for all categories, after harmonizing country names.
    ds_population : Dataset
        Population dataset.
    ds_regions : Dataset
        Regions dataset.
    ds_income_groups : Dataset
        Income groups dataset.

    Returns
    -------
    table : Table
        Table of processed data for the given category.

    """
    # Prepare wide table.
    table = make_wide_table(
        tb=tb, category="Electricity demand", ds_regions=ds_regions, ds_income_groups=ds_income_groups
    )

    # Add population to data
    table = geo.add_population_to_table(tb=table, ds_population=ds_population, warn_on_missing_countries=False)

    # Recalculate demand per capita.
    # We could do this only for region aggregates (since they do not have per capita values),
    # but we do this for all countries, to ensure per-capita variables are consistent with our population data.
    table["Demand per capita - kWh"] = table["Demand - TWh"] * TWH_TO_KWH / table["population"]

    # Delete the original demand per capita column.
    table = table.drop(columns=["Demand per capita - MWh"], errors="raise")

    return table


def make_table_power_sector_emissions(tb: Table, ds_regions: Dataset, ds_income_groups: Dataset) -> Table:
    """Create table with processed data of category "Power sector emissions".

    Parameters
    ----------
    tb : Table
        Data in long format for all categories, after harmonizing country names.
    ds_regions : Dataset
        Regions dataset.
    ds_income_groups : Dataset
        Income groups dataset.

    Returns
    -------
    table : Table
        Table of processed data for the given category.

    """
    # Prepare wide table of emissions data.
    table = make_wide_table(
        tb=tb, category="Power sector emissions", ds_regions=ds_regions, ds_income_groups=ds_income_groups
    )

    # Add carbon intensity.
    # In principle this only needs to be done for region aggregates, but we do it for all countries and check that
    # the results are consistent with the original data.
    # Prepare wide table also for electricity generation (required to calculate carbon intensity).
    electricity = make_wide_table(
        tb=tb, category="Electricity generation", ds_regions=ds_regions, ds_income_groups=ds_income_groups
    )[["country", "year", "Total Generation - TWh"]]
    # Add total electricity generation to emissions table.
    table = pr.merge(table, electricity, on=["country", "year"], how="left")
    # Rename the original carbon intensity column as a temporary column called "check".
    intensity_col = "CO2 intensity - gCO2/kWh"
    table = table.rename(columns={intensity_col: "check"}, errors="raise")
    # Calculate carbon intensity for all countries and regions.
    table[intensity_col] = table["Total emissions - mtCO2"] * MT_TO_G / (table["Total Generation - TWh"] * TWH_TO_KWH)

    # Check that the new carbon intensities agree (within 1 % of mean average percentage error, aka mape) with the
    # original ones (where carbon intensity was given, namely for countries, not aggregate regions).
    mape = 100 * abs(table.dropna(subset="check")[intensity_col] - table["check"].dropna()) / table["check"].dropna()
    assert mape.max() < 1, "Calculated carbon intensities differ from original ones by more than 1 percent."

    # Remove temporary column.
    table = table.drop(columns=["check"], errors="raise")

    return table


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load dataset from meadow and read its main table.
    ds_meadow = paths.load_dataset("yearly_electricity")
    tb_meadow = ds_meadow["yearly_electricity"].reset_index()

    # Load population dataset.
    ds_population = paths.load_dataset("population")

    # Load regions dataset.
    ds_regions = paths.load_dataset("regions")

    # Load income groups dataset.
    ds_income_groups = paths.load_dataset("income_groups")

    #
    # Process data.
    #
    # Select and rename columns conveniently.
    tb = tb_meadow[list(COLUMNS_YEARLY_ELECTRICITY)].rename(columns=COLUMNS_YEARLY_ELECTRICITY, errors="raise")

    # Sanity check.
    assert set(tb["category"]) == set(CATEGORIES), "Categories have changed in data."

    # Harmonize country names.
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
        excluded_countries_file=paths.excluded_countries_path,
        warn_on_missing_countries=True,
        warn_on_unused_countries=True,
    )

    # Split data into different tables, one per category, and process each one individually.
    tables = {
        "capacity": make_wide_table(
            tb=tb, category="Capacity", ds_regions=ds_regions, ds_income_groups=ds_income_groups
        ),
        "electricity_demand": make_table_electricity_demand(
            tb=tb, ds_population=ds_population, ds_regions=ds_regions, ds_income_groups=ds_income_groups
        ),
        "electricity_generation": make_table_electricity_generation(
            tb=tb, ds_regions=ds_regions, ds_income_groups=ds_income_groups
        ),
        "electricity_imports": make_wide_table(
            tb=tb, category="Electricity imports", ds_regions=ds_regions, ds_income_groups=ds_income_groups
        ),
        "power_sector_emissions": make_table_power_sector_emissions(
            tb=tb, ds_regions=ds_regions, ds_income_groups=ds_income_groups
        ),
    }

    for table_name in tables:
        # Set a table short name.
        tables[table_name].metadata.short_name = table_name
        # Ensure all columns are snake-case, set an appropriate index and sort conveniently.
        tables[table_name] = (
            tables[table_name].underscore().set_index(["country", "year"], verify_integrity=True).sort_index()
        )

    ####################################################################################################################
    # The data for many regions presents a big drop in the last year, simply because many countries are not informed.
    # Assert that this drop exists, and remove the last data point for regions.
    error = (
        "Expected a big drop in the last data point for regions (because of limited data availability)."
        "If that is no longer the case, remove this part of the code and keep the last data points for regions."
    )
    assert tables["capacity"].loc["Africa"]["renewables__gw"].diff().iloc[-1] < -30, error
    for table_name in tables:
        latest_year = tables[table_name].reset_index()["year"].max()
        for column in tables[table_name].columns:
            for region in geo.REGIONS:
                # tables[table_name] = tables[table_name].copy()
                tables[table_name].loc[(region, latest_year), column] = None
    ####################################################################################################################

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(
        dest_dir, tables=tables.values(), default_metadata=ds_meadow.metadata, check_variables_metadata=True
    )
    ds_garden.save()
