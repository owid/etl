"""Garden step for Ember's Yearly Electricity Data (combining global and European data)."""

from typing import Dict

import owid.catalog.processing as pr
from owid.catalog import Dataset, Table, utils
from owid.datautils.dataframes import combine_two_overlapping_dataframes
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder

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
CATEGORIES_GLOBAL = [
    "Capacity",
    "Electricity demand",
    "Electricity generation",
    "Electricity imports",
    "Power sector emissions",
]
CATEGORIES_EUROPE = [
    "Electricity demand",
    "Electricity generation",
    "Electricity imports",
    "Power sector emissions",
]
# Subcategories expected to exist in the data.
SUBCATEGORIES = [
    "Aggregate fuel",
    "CO2 intensity",
    "Demand",
    "Demand per capita",
    "Electricity imports",
    "Fuel",
    "Total",
]
# Variables expected to exist in the data.
VARIABLES_GLOBAL = [
    "Bioenergy",
    "CO2 intensity",
    "Clean",
    "Coal",
    "Demand",
    "Demand per capita",
    "Fossil",
    "Gas",
    "Gas and Other Fossil",
    "Hydro",
    "Hydro, Bioenergy and Other Renewables",
    "Net Imports",
    "Nuclear",
    "Other Fossil",
    "Other Renewables",
    "Renewables",
    "Solar",
    "Total Generation",
    "Total emissions",
    "Wind",
    "Wind and Solar",
]
VARIABLES_EUROPE = [
    "Bioenergy",
    "CO2 intensity",
    "Clean",
    "Coal",
    "Demand",
    "Demand per capita",
    "Fossil",
    "Gas",
    "Hard coal",
    "Hydro",
    "Hydro, bioenergy and other renewables",
    "Lignite",
    "Net imports",
    "Nuclear",
    "Offshore wind",
    "Onshore wind",
    "Other fossil",
    "Other renewables",
    "Renewables",
    "Solar",
    "Total generation",
    "Wind",
    "Wind and solar",
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
    "Gas and other fossil - %",
    "Gas and other fossil - GW",
    "Gas and other fossil - TWh",
    "Gas and other fossil - mtCO2",
    # "Hydro - %",
    "Hydro - GW",
    "Hydro - TWh",
    "Hydro - mtCO2",
    "Hydro, bioenergy and other renewables - %",
    "Hydro, bioenergy and other renewables - GW",
    "Hydro, bioenergy and other renewables - TWh",
    "Hydro, bioenergy and other renewables - mtCO2",
    "Net imports - TWh",
    # "Nuclear - %",
    "Nuclear - GW",
    "Nuclear - TWh",
    "Nuclear - mtCO2",
    # "Other Fossil - %",
    "Other fossil - GW",
    "Other fossil - TWh",
    "Other fossil - mtCO2",
    # "Other Renewables - %",
    "Other renewables - GW",
    "Other renewables - TWh",
    "Other renewables - mtCO2",
    # "Renewables - %",
    "Renewables - GW",
    "Renewables - TWh",
    "Renewables - mtCO2",
    # "Solar - %",
    "Solar - GW",
    "Solar - TWh",
    "Solar - mtCO2",
    "Total generation - TWh",
    "Total emissions - mtCO2",
    # "Wind - %",
    "Wind - GW",
    "Wind - TWh",
    "Wind - mtCO2",
    # "Wind and Solar - %",
    "Wind and solar - GW",
    "Wind and solar - TWh",
    "Wind and solar - mtCO2",
]


def sanity_check_inputs(tb_global: Table, tb_europe: Table) -> None:
    assert set(tb_global.columns) == set(tb_europe.columns), "Columns in global and European data have changed."
    assert set(tb_global["category"]) == set(CATEGORIES_GLOBAL), "Categories have changed in data."
    assert set(tb_europe["category"]) == set(CATEGORIES_EUROPE), "Categories have changed in data."
    assert set(tb_global["subcategory"]) == set(SUBCATEGORIES), "Subcategories have changed in data."
    assert set(tb_europe["subcategory"]) == set(SUBCATEGORIES), "Subcategories have changed in data."
    assert set(tb_global["variable"]) == set(VARIABLES_GLOBAL), "Variables have changed in global data."
    assert set(tb_europe["variable"]) == set(VARIABLES_EUROPE), "Variables have changed in data."


def prepare_input_data(tb: Table) -> Table:
    tb = tb.copy()

    # Select and rename columns conveniently.
    tb = tb[list(COLUMNS_YEARLY_ELECTRICITY)].rename(columns=COLUMNS_YEARLY_ELECTRICITY, errors="raise")

    # Harmonize names of variables, categories and subcategories.
    for field in ["variable", "category", "subcategory"]:
        tb[field] = [value.capitalize().replace("Co2", "CO2") for value in tb[field]]

    # Harmonize country names.
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
        warn_on_missing_countries=True,
        # For debugging, set it to True (the only unused country in global data should be "Türkiye", which is only used in European data).
        warn_on_unused_countries=False,
    )

    return tb


def combine_global_and_europe_data(tb_global: Table, tb_europe: Table) -> Table:
    # These are the main differences between the two datasets:
    # - Variables in global data are title-cased and in European data are sentence-cased.
    # - Global data includes category 'Capacity', while European does not.
    # - Global data includes variable 'Total emissions', while European does not.
    # - European data includes the following subcategory-variables pairs:
    #   'Aggregate fuel - Coal',
    #   'Aggregate fuel - Wind',
    #   'Fuel - Hard coal',
    #   'Fuel - Lignite',
    #   'Fuel - Offshore wind',
    #   'Fuel - Onshore wind',
    # - Meanwhile global data only includes:
    #   'Aggregate fuel - Gas and other fossil',
    #   'Fuel - Coal',
    #   'Fuel - Wind',
    # - Global data includes data for all European countries from 2000 onwards. European data includes the same data from 2000 onwards, but also data from 1990 to 1999.

    error = "Variables in global and European data have changed."
    assert set(tb_global["variable"]) - set(tb_europe["variable"]) == {
        "Gas and other fossil",
        "Total emissions",
    }, error
    assert set(tb_europe["variable"]) - set(tb_global["variable"]) == {
        "Hard coal",
        "Lignite",
        "Offshore wind",
        "Onshore wind",
    }, error

    # The simplest solution regarding coal and wind is to rename the subcategory of European data from "Aggregate fuel" to "Fuel".
    tb_europe.loc[
        (tb_europe["subcategory"] == "Aggregate fuel") & (tb_europe["variable"].isin(["Coal", "Wind"])),
        "subcategory",
    ] = "Fuel"

    # Create the gas and other fossil aggregate for European data.
    error = "Expected European data to not include 'Gas and other fossil' variable."
    assert not (tb_europe["variable"] == "Gas and other fossil").any(), error
    tb_europe_gas_and_other_fossil = (
        tb_europe[(tb_europe["variable"].isin(["Gas", "Other fossil"])) & (tb_europe["unit"].isin(["TWh", "MtCO2e"]))]
        .groupby(["country", "year", "unit", "category"], as_index=False)
        .agg({"value": "sum"})
        .assign(**{"variable": "Gas and other fossil", "subcategory": "Aggregate fuel"})
    )
    tb_europe = pr.concat([tb_europe, tb_europe_gas_and_other_fossil], ignore_index=True)

    # Check that the category-subcategory-variable groups are now identical for global and European data.
    set_global = set(
        [
            t["category"] + " - " + t["subcategory"] + " - " + t["variable"]
            for _, t in tb_global[(tb_global["category"] != "Capacity") & (tb_global["variable"] != "Total emissions")][
                ["category", "subcategory", "variable"]
            ]
            .drop_duplicates()
            .iterrows()
        ]
    )
    set_europe = set(
        [
            t["category"] + " - " + t["subcategory"] + " - " + t["variable"]
            for _, t in tb_europe[
                ~tb_europe["variable"].isin(["Hard coal", "Lignite", "Onshore wind", "Offshore wind"])
            ][["category", "subcategory", "variable"]]
            .drop_duplicates()
            .iterrows()
        ]
    )
    assert (
        set_global == set_europe
    ), "After adapting European data, all category-subcategory-variables should be identical, except for:\n* Capacity and total emissions (only given in global), and\n* Hard coal, Lignite, Onshore wind and Offshore wind, only given in European data."

    # Combine the two overlapping datasets, prioritizing European on overlapping rows.
    tb = combine_two_overlapping_dataframes(
        df1=tb_europe,
        df2=tb_global,
        index_columns=["country", "year", "variable", "unit", "category", "subcategory"],
    )

    return tb


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
    # Sanity check.
    error = "Combinations of variable and unit expected to create aggregates are missing."
    assert set(SUM_AGGREGATES) < set(tb["variable"] + " - " + tb["unit"]), error

    # Select data for given category.
    _tb = tb[tb["category"] == category].copy()

    # Pivot table to have a column for each variable.
    table = _tb.pivot(
        index=["country", "year"],
        columns=["variable", "unit"],
        values="value",
        join_column_levels_with=" - ",
        fill_dimensions=False,
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
            table.loc[select_regions, column] = table[value_column] / table["Total generation - TWh"] * 100

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
    )[["country", "year", "Total generation - TWh"]]
    # Add total electricity generation to emissions table.
    table = pr.merge(table, electricity, on=["country", "year"], how="left")
    # Rename the original carbon intensity column as a temporary column called "check".
    intensity_col = "CO2 intensity - gCO2/kWh"
    table = table.rename(columns={intensity_col: "check"}, errors="raise")
    # Calculate carbon intensity for all countries and regions.
    table[intensity_col] = table["Total emissions - mtCO2"] * MT_TO_G / (table["Total generation - TWh"] * TWH_TO_KWH)

    # Check that the new carbon intensities agree (within 1 % of mean average percentage error, aka mape) with the
    # original ones (where carbon intensity was given, namely for countries, not aggregate regions).
    mape = 100 * abs(table.dropna(subset="check")[intensity_col] - table["check"].dropna()) / table["check"].dropna()
    assert mape.max() < 1, "Calculated carbon intensities differ from original ones by more than 1 percent."

    # Remove temporary column.
    table = table.drop(columns=["check"], errors="raise")

    return table


def combine_yearly_electricity_data(tables: Dict[str, Table]) -> Table:
    """Combine all tables in Ember's Yearly Electricity Data into one table.

    Parameters
    ----------
    tables : List[Table]
        Yearly Electricity data (containing tables for capacity, electricity demand, generation, imports and
        emissions).

    Returns
    -------
    tb_combined : Table
        Combined table containing all data in the Yearly Electricity dataset.

    """
    category_renaming = {
        "capacity": "Capacity - ",
        "electricity_demand": "",
        "electricity_generation": "Generation - ",
        "electricity_imports": "",
        "power_sector_emissions": "Emissions - ",
    }
    error = "Tables in yearly electricity dataset have changed"
    assert set(category_renaming) == set(tables), error
    index_columns = ["country", "year"]
    for table_name in list(tables):
        tables[table_name] = (
            tables[table_name]
            .reset_index()
            .rename(
                columns={
                    column: utils.underscore(category_renaming[table_name] + column)
                    for column in tables[table_name].columns
                    if column not in index_columns
                },
                errors="raise",
            )
        )

    # Merge all tables into one, with an appropriate short name.
    tb_combined = pr.multi_merge(list(tables.values()), on=index_columns, how="outer", short_name=paths.short_name)  # type: ignore

    # Rename certain columns for consistency.
    tb_combined = tb_combined.rename(
        columns={
            "net_imports__twh": "imports__total_net_imports__twh",
            "demand__twh": "demand__total_demand__twh",
            "demand_per_capita__kwh": "demand__total_demand_per_capita__kwh",
        },
        errors="raise",
    )

    # Sanity check.
    error = "Total generation column in emissions and generation tables are not identical."
    assert all(
        tb_combined["emissions__total_generation__twh"].fillna(-1)
        == tb_combined["generation__total_generation__twh"].fillna(-1)
    ), error

    # Remove unnecessary columns and any possible rows with no data.
    tb_combined = tb_combined.drop(columns=["population", "emissions__total_generation__twh"], errors="raise").dropna(
        how="all"
    )

    # Set a convenient index and sort rows and columns conveniently.
    tb_combined = tb_combined.format(sort_columns=True)

    return tb_combined


def run() -> None:
    #
    # Load data.
    #
    # Load dataset from meadow and read its main table.
    ds_meadow = paths.load_dataset("yearly_electricity")
    tb_global = ds_meadow.read("yearly_electricity__global")
    tb_europe = ds_meadow.read("yearly_electricity__europe")

    # Load population dataset.
    ds_population = paths.load_dataset("population")

    # Load regions dataset.
    ds_regions = paths.load_dataset("regions")

    # Load income groups dataset.
    ds_income_groups = paths.load_dataset("income_groups")

    #
    # Process data.
    #
    # Sanity check inputs.
    sanity_check_inputs(tb_global=tb_global, tb_europe=tb_europe)

    # Prepare global and European input data.
    tb_global = prepare_input_data(tb=tb_global)
    tb_europe = prepare_input_data(tb=tb_europe)

    # Combine global and European data.
    tb = combine_global_and_europe_data(tb_global=tb_global, tb_europe=tb_europe)

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
        # Prepare table's format.
        tables[table_name] = tables[table_name].format(short_name=table_name)

    ####################################################################################################################
    # The data for many regions presents a big drop in the last year, simply because many countries are not informed.
    # Assert that this drop exists, and remove the last data point for regions.
    error = (
        "Expected a big drop in the last data point for regions (because of limited data availability)."
        "If that is no longer the case, remove this part of the code and keep the last data points for regions."
    )
    assert tables["capacity"].loc["Africa"]["renewables__gw"].diff().iloc[-1] < -29, error
    for table_name in tables:
        latest_year = tables[table_name].reset_index()["year"].max()
        for column in tables[table_name].columns:
            for region in geo.REGIONS:
                tables[table_name].loc[(region, latest_year), column] = None

    # Similarly, data prior to 2000 exists only for European countries.
    # This can cause spurious jump in aggregate data.
    # For example, there is a spurious jump from 1999 to 2000 for Upper-middle-income countries
    # (see e.g. "Renewables - TWh"), because prior to 2000 only a few UMI countries have data.
    # Assert that this jump exists, and remove aggregate data prior to 2000 (except European aggregates).
    error = (
        "Expected a big jump (>1000%) (in e.g. renewable generation) between 1999 and 2000 for Upper-middle-income "
        "countries (because prior to 2000 only Ukraine has data). If that is no longer the case (because not only "
        "European countries are informed prior to 2000), remove this part of the code."
    )
    renewables_umic_1999 = tables["electricity_generation"].loc["Upper-middle-income countries", 1999][
        "renewables__twh"
    ]
    renewables_umic_2000 = tables["electricity_generation"].loc["Upper-middle-income countries", 2000][
        "renewables__twh"
    ]
    assert 100 * (renewables_umic_2000 - renewables_umic_1999) / renewables_umic_1999 > 1000, error
    # We could still create European aggregates, but certain European countries are also missing data prior to 2000.
    # It seems safer to make nan all aggregate data in all yearly electricity tables prior to 2000.
    for table_name in tables:
        for column in tables[table_name].columns:
            tables[table_name].loc[
                (tables[table_name].index.get_level_values(0).isin(geo.REGIONS))
                & (tables[table_name].index.get_level_values(1) < 2000),
                :,
            ] = None
    ####################################################################################################################

    # Combine all tables into one.
    tb_combined = combine_yearly_electricity_data(tables=tables)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb_combined])
    ds_garden.save()
