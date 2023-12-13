"""Garden step for Ember's Yearly Electricity Data.

"""

import itertools
from typing import Any, Dict, List, Tuple

import numpy as np
import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Dataset, Table
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
                "Clean - GW": np.nan,
                "Fossil - GW": np.nan,
                "Gas and Other Fossil - GW": np.nan,
                "Hydro, Bioenergy and Other Renewables - GW": np.nan,
                "Renewables - GW": np.nan,
                "Wind and Solar - GW": np.nan,
                "Bioenergy - GW": np.nan,
                "Coal - GW": np.nan,
                "Gas - GW": np.nan,
                "Hydro - GW": np.nan,
                "Nuclear - GW": np.nan,
                "Other Fossil - GW": np.nan,
                "Other Renewables - GW": np.nan,
                "Solar - GW": np.nan,
                "Wind - GW": np.nan,
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
                "Demand - TWh": np.nan,
                "population": np.nan,
                "Demand per capita - kWh": np.nan,
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
                "Clean - %": np.nan,
                "Fossil - %": np.nan,
                "Gas and Other Fossil - %": np.nan,
                "Hydro, Bioenergy and Other Renewables - %": np.nan,
                "Renewables - %": np.nan,
                "Wind and Solar - %": np.nan,
                "Clean - TWh": np.nan,
                "Fossil - TWh": np.nan,
                "Gas and Other Fossil - TWh": np.nan,
                "Hydro, Bioenergy and Other Renewables - TWh": np.nan,
                "Renewables - TWh": np.nan,
                "Wind and Solar - TWh": np.nan,
                "Bioenergy - %": np.nan,
                "Coal - %": np.nan,
                "Gas - %": np.nan,
                "Hydro - %": np.nan,
                "Nuclear - %": np.nan,
                "Other Fossil - %": np.nan,
                "Other Renewables - %": np.nan,
                "Solar - %": np.nan,
                "Wind - %": np.nan,
                "Bioenergy - TWh": np.nan,
                "Coal - TWh": np.nan,
                "Gas - TWh": np.nan,
                "Hydro - TWh": np.nan,
                "Nuclear - TWh": np.nan,
                "Other Fossil - TWh": np.nan,
                "Other Renewables - TWh": np.nan,
                "Solar - TWh": np.nan,
                "Wind - TWh": np.nan,
                "Total Generation - TWh": np.nan,
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
                "Net Imports - TWh": np.nan,
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
                "Clean - mtCO2": np.nan,
                "Fossil - mtCO2": np.nan,
                "Gas and Other Fossil - mtCO2": np.nan,
                "Hydro, Bioenergy and Other Renewables - mtCO2": np.nan,
                "Renewables - mtCO2": np.nan,
                "Wind and Solar - mtCO2": np.nan,
                "Bioenergy - mtCO2": np.nan,
                "Coal - mtCO2": np.nan,
                "Gas - mtCO2": np.nan,
                "Hydro - mtCO2": np.nan,
                "Nuclear - mtCO2": np.nan,
                "Other Fossil - mtCO2": np.nan,
                "Other Renewables - mtCO2": np.nan,
                "Solar - mtCO2": np.nan,
                "Wind - mtCO2": np.nan,
                "Total emissions - mtCO2": np.nan,
                "Total Generation - TWh": np.nan,
                "CO2 intensity - gCO2/kWh": np.nan,
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


def _expand_combinations_in_amendments(
    amendments: List[Tuple[Dict[Any, Any], Dict[Any, Any]]]
) -> List[Tuple[Dict[Any, Any], Dict[Any, Any]]]:
    """When values in amendments are given as lists, explode them to have all possible combinations of values."""
    amendments_expanded = []
    for wrong_row, corrected_row in amendments:
        field, values = zip(*wrong_row.items())
        for amendment_single in [dict(zip(field, value)) for value in itertools.product(*values)]:
            amendments_expanded.append((amendment_single, corrected_row))

    return amendments_expanded


def correct_data_points(tb: Table, corrections: List[Tuple[Dict[Any, Any], Dict[Any, Any]]]) -> Table:
    """Make individual corrections to data points in a table.

    Parameters
    ----------
    tb : Table
        Data to be corrected.
    corrections : List[Tuple[Dict[Any, Any], Dict[Any, Any]]]
        Corrections.

    Returns
    -------
    tb_corrected : Table
        Corrected data.

    """
    tb_corrected = tb.copy()

    corrections_expanded = _expand_combinations_in_amendments(amendments=corrections)
    for wrong_row, corrected_row in corrections_expanded:
        # Select the row in the table where the wrong data point is.
        # The 'fillna(False)' is added because otherwise rows that do not fulfil the selection will create ambiguity.
        selection = tb_corrected.loc[(tb_corrected[list(wrong_row)] == pd.Series(wrong_row)).fillna(False).all(axis=1)]
        # Sanity check.
        error = "Either raw data has been corrected, or dictionary selecting wrong row is ambiguous."
        assert len(selection) == 1, error

        # Replace wrong fields by the corrected ones.
        # Note: Changes to categorical fields will not work.
        tb_corrected.loc[selection.index, list(corrected_row)] = list(corrected_row.values())

    return tb_corrected


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
            select_regions = table["country"].isin(list(REGIONS))
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

    # Apply amendments, and set an appropriate index and short name to each table an sort conveniently.
    # TODO: Instead of this AMENDMENTS, simply assert that there is a significant decrease in region aggregates in the
    #   last year, and remove points.
    for table_name in tables:
        if table_name in AMENDMENTS:
            log.info(f"Applying amendments to table: {table_name}")
            tables[table_name] = correct_data_points(tb=tables[table_name], corrections=AMENDMENTS[table_name])
        tables[table_name] = tables[table_name].set_index(["country", "year"], verify_integrity=True).sort_index()
        tables[table_name].metadata.short_name = table_name

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(
        dest_dir, tables=tables.values(), default_metadata=ds_meadow.metadata, check_variables_metadata=True
    )
    ds_garden.save()
