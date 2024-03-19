"""Shared definitions in FAOSTAT garden steps.

This module contains:
* Common functions used in garden steps.
* Definitions related to elements and items (e.g. item amendments).
* Definitions related to countries and regions (e.g. aggregate regions to generate and definition of historic regions).
* Definitions of flags (found in the original FAOSTAT data) and their ranking (i.e. the priority of data points when
  there are duplicates).
* Other additional definitions (e.g. texts to include in the definitions of generated per-capita variables).

"""

import json
import sys
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import owid.catalog.processing as pr
import pandas as pd
import structlog
from detected_anomalies import handle_anomalies
from owid import repack  # type: ignore
from owid.catalog import Dataset, Table, Variable, VariablePresentationMeta
from owid.catalog.utils import underscore
from owid.datautils import dataframes
from tqdm.auto import tqdm

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Initialise log.
log = structlog.get_logger()

# Define path to current folder, namespace and version of all datasets in this folder.
CURRENT_DIR = Path(__file__).parent
NAMESPACE = CURRENT_DIR.parent.name
VERSION = CURRENT_DIR.name

# Name of FAOSTAT metadata dataset.
FAOSTAT_METADATA_SHORT_NAME = f"{NAMESPACE}_metadata"

# Elements and items.

# Maximum number of characters for item_code.
# FAOSTAT "item_code" is usually an integer number, however sometimes it has decimals and sometimes it contains letters.
# So we will convert it into a string of this number of characters (integers will be prepended with zeros).
N_CHARACTERS_ITEM_CODE = 8
# Maximum number of characters for item_code for faostat_sdgb and faostat_fs, which have a different kind of item codes,
# e.g. '24002-F-Y_GE15', '24002-M-Y_GE15', etc.
N_CHARACTERS_ITEM_CODE_EXTENDED = 15
# Maximum number of characters for element_code (integers will be prepended with zeros).
N_CHARACTERS_ELEMENT_CODE = 6
# Manual fixes to item codes to avoid ambiguities.
ITEM_AMENDMENTS = {
    "faostat_fbsh": [
        # Mappings to harmonize item names of fbsh with those of fbs.
        {
            "item_code": "00002556",
            "fao_item": "Groundnuts (Shelled Eq)",
            "new_item_code": "00002552",
            "new_fao_item": "Groundnuts",
        },
        {
            "item_code": "00002805",
            "fao_item": "Rice (Milled Equivalent)",
            "new_item_code": "00002807",
            "new_fao_item": "Rice and products",
        },
    ],
}
# Manual fixes to elements and units to avoid ambiguities.
ELEMENT_AMENDMENTS = {
    "faostat_fbsh": [
        # Mappings to harmonize element and unit names of fbsh with those of fbs.
        {
            "element_code": "000645",
            "fao_element": "Food supply quantity (kg/capita/yr)",
            "fao_unit": "kg/cap",
            "new_element_code": "000645",
            "new_fao_element": "Food supply quantity (kg/capita/yr)",
            "new_fao_unit": "kg",
        },
    ],
}
# Ideally, all elements of fbsh should be in fbs (although fbs may contain additional elements).
# The only exception is "Stock Variation", which have slightly different definitions:
# On fbs "Stock Variation" (005072), "Net decreases (from stock) are generally indicated by the sign "-". No sign denotes net increases (add to stock)".
# On fbsh "Stock Variation" (005074), "Net increases in stocks (add to stock) are generally indicated by the sign "-". No sign denotes net decreases (from stock).".
# Given that they have different definitions, we should not map one to the other.
# So, for now, simply ignore it.
ELEMENTS_IN_FBSH_MISSING_IN_FBS = {"005074"}

# Countries and regions.

# When creating region aggregates for a certain variable in a certain year, we want to ensure that we have enough
# data to create the aggregate. There is no straightforward way to do so. Our criterion is to:
#  * sum the data of all countries in the region, and then
#  * remove rows such that the sum of the population of countries with data (for a given year) is too small, compared
#    to the total population of the region.
# For example, if for a certain variable in a certain year, only a few countries with little population have data,
# then assign nan to that region-variable-year.
# Define here that minimum fraction of population that must have data to create an aggregate.
# A fraction of 0 means that we do accept aggregates even if only a few countries contribute (which seems to be the
# default approach by FAOSTAT).
MIN_FRAC_POPULATION_WITH_DATA = 0.0
# Reference year to build the list of mandatory countries.
REFERENCE_YEAR = 2018
REGIONS_TO_ADD = {
    "North America": {
        "area_code": "OWID_NAM",
        "min_frac_population_with_data": MIN_FRAC_POPULATION_WITH_DATA,
    },
    "South America": {
        "area_code": "OWID_SAM",
        "min_frac_population_with_data": MIN_FRAC_POPULATION_WITH_DATA,
    },
    "Europe": {
        "area_code": "OWID_EUR",
        "min_frac_population_with_data": MIN_FRAC_POPULATION_WITH_DATA,
    },
    "European Union (27)": {
        "area_code": "OWID_EU27",
        "min_frac_population_with_data": MIN_FRAC_POPULATION_WITH_DATA,
    },
    "Africa": {
        "area_code": "OWID_AFR",
        "min_frac_population_with_data": MIN_FRAC_POPULATION_WITH_DATA,
    },
    "Asia": {
        "area_code": "OWID_ASI",
        "min_frac_population_with_data": MIN_FRAC_POPULATION_WITH_DATA,
    },
    "Oceania": {
        "area_code": "OWID_OCE",
        "min_frac_population_with_data": MIN_FRAC_POPULATION_WITH_DATA,
    },
    "Low-income countries": {
        "area_code": "OWID_LIC",
        "min_frac_population_with_data": MIN_FRAC_POPULATION_WITH_DATA,
    },
    "Upper-middle-income countries": {
        "area_code": "OWID_UMC",
        "min_frac_population_with_data": MIN_FRAC_POPULATION_WITH_DATA,
    },
    "Lower-middle-income countries": {
        "area_code": "OWID_LMC",
        "min_frac_population_with_data": MIN_FRAC_POPULATION_WITH_DATA,
    },
    "High-income countries": {
        "area_code": "OWID_HIC",
        "min_frac_population_with_data": MIN_FRAC_POPULATION_WITH_DATA,
    },
}

# When creating region aggregates, we need to ignore geographical regions that contain aggregate data from other
# countries, to avoid double-counting the data of those countries.
# Note: This list does not contain all country groups, but only those that are in our list of harmonized countries
# (without the *(FAO) suffix).
REGIONS_TO_IGNORE_IN_AGGREGATES = []

# When creating region aggregates, decide how to distribute historical regions.
# The following decisions are based on the current location of the countries that succeeded the region, and their income
# group. Continent and income group assigned corresponds to the continent and income group of the majority of the
# population in the member countries.
HISTORIC_TO_CURRENT_REGION = {
    "Czechoslovakia": {
        "continent": "Europe",
        "income_group": "High-income countries",
        "members": [
            # Europe - High-income countries.
            "Czechia",
            "Slovakia",
        ],
    },
    "Ethiopia (former)": {
        "continent": "Africa",
        "income_group": "Low-income countries",
        "members": [
            # Africa - Low-income countries.
            "Ethiopia",
            "Eritrea",
        ],
    },
    "Netherlands Antilles": {
        "continent": "North America",
        "income_group": "High-income countries",
        "members": [
            # North America - High-income countries.
            "Aruba",
            "Curacao",
            "Sint Maarten (Dutch part)",
        ],
    },
    "Serbia and Montenegro": {
        "continent": "Europe",
        "income_group": "Upper-middle-income countries",
        "members": [
            # Europe - Upper-middle-income countries.
            "Serbia",
            "Montenegro",
        ],
    },
    "Sudan (former)": {
        "continent": "Africa",
        "income_group": "Low-income countries",
        "members": [
            # Africa - Low-income countries.
            "Sudan",
            "South Sudan",
        ],
    },
    "USSR": {
        "continent": "Europe",
        "income_group": "Upper-middle-income countries",
        "members": [
            # Europe - High-income countries.
            "Lithuania",
            "Estonia",
            "Latvia",
            # Europe - Upper-middle-income countries.
            "Moldova",
            "Belarus",
            "Russia",
            # Europe - Lower-middle-income countries.
            "Ukraine",
            # Asia - Upper-middle-income countries.
            "Georgia",
            "Armenia",
            "Azerbaijan",
            "Turkmenistan",
            "Kazakhstan",
            # Asia - Lower-middle-income countries.
            "Kyrgyzstan",
            "Uzbekistan",
            "Tajikistan",
        ],
    },
    "Yugoslavia": {
        "continent": "Europe",
        "income_group": "Upper-middle-income countries",
        "members": [
            # Europe - High-income countries.
            "Croatia",
            "Slovenia",
            # Europe - Upper-middle-income countries.
            "North Macedonia",
            "Bosnia and Herzegovina",
            "Serbia",
            "Montenegro",
        ],
    },
}


# Flags.

# We have created a manual ranking of FAOSTAT flags. These flags are only used when there is ambiguity in the data,
# namely, when there is more than one data value for a certain country-year-item-element-unit.
# NOTES:
# * We check that the definitions in our manual ranking agree with the ones provided by FAOSTAT.
# * We do not include all flags: We include only the ones that solve an ambiguity in a particular case, and add more
#   flags as we see need.
# * We have found flags that appeared in a dataset, but were not included in the additional metadata
#   (namely flag "R", found in qcl dataset, and "W" in rt dataset). These flags were added manually, using the
#   definition in List / Flags in:
#   https://www.fao.org/faostat/en/#definitions
# * Other flags (namely "B", in rl dataset and "w" in rt dataset) were not found either in the additional metadata or in
#   the website definitions. They have been assigned the description "Unknown flag".
# * Unfortunately, flags do not remove all ambiguities: remaining duplicates are dropped without any meaningful
#   criterion.
# Flag to assign to data points with nan flag (which by definition is considered official data).
FLAG_OFFICIAL_DATA = "official_data"
# Flag to assign to data points for regions that are the result of aggregating data points with different flags.
FLAG_MULTIPLE_FLAGS = "multiple_flags"
# Rank flags by priority (where lowest index is highest priority).
FLAGS_RANKING = (
    pr.read_from_records(
        columns=["flag", "description"],
        data=[
            # FAO uses nan flag for official data; in our datasets we will replace nans by FLAG_OFFICIAL_DATA.
            (np.nan, "Official data"),
            ("A", "Official figure"),
            ("X", "Figure from international organizations"),
            ("C", "Aggregate, may include official, semi-official, estimated or calculated data"),
            ("P", "Provisional value"),
            ("I", "Imputed value"),
            ("E", "Estimated value"),
            ("F", "Forecast value"),
            ("T", "Unofficial figure"),
            ("B", "Time series break"),
            ("N", "Not significant (negligible)"),
            ("U", "Low reliability"),
            ("G", "Experimental value"),
            ("L", "Missing value; data exist"),
            ("O", "Missing value"),
            ("M", "Missing value (data cannot exist, not applicable)"),
            ("Q", "Missing value; suppressed"),
            ("V", "Unvalidated value"),
            ("Fp", "Unknown flag"),
        ],
    )
    .reset_index()
    .rename(columns={"index": "ranking"})
)

# Additional descriptions.

# Additional explanation to append to element description for variables that were originally given per capita.
WAS_PER_CAPITA_ADDED_ELEMENT_DESCRIPTION = (
    "Originally given per-capita, and converted into total figures by " "multiplying by population (given by FAO)."
)
# Additional explanation to append to element description for created per-capita variables.
NEW_PER_CAPITA_ADDED_ELEMENT_DESCRIPTION = (
    "Per-capita values are obtained by dividing the original values by the "
    "population (either provided by FAO or by OWID)."
)

# Additional text to include in the metadata title of the output wide table.
ADDED_TITLE_TO_WIDE_TABLE = " - Flattened table indexed by country-year."

# Name of item, element and unit of FAO population (used to select population in the data).
FAO_POPULATION_ITEM_NAME = "Population"
FAO_POPULATION_ELEMENT_NAME = "Total Population - Both sexes"
FAO_POPULATION_UNIT_NAME = "thousand Number"

# Shared functions.


def check_that_countries_are_well_defined(tb: Table) -> None:
    """Apply sanity checks related to the definition of countries.

    Parameters
    ----------
    tb : Table
        Data, right after harmonizing country names.

    """
    # Ensure area codes and countries are well defined, and no ambiguities were introduced when mapping country names.
    n_countries_per_area_code = tb.groupby("area_code")["country"].transform("nunique")
    ambiguous_area_codes = (
        tb.loc[n_countries_per_area_code > 1][["area_code", "country"]]
        .drop_duplicates()
        .set_index("area_code")["country"]
        .to_dict()
    )
    error = (
        f"There cannot be multiple countries for the same area code. "
        f"Redefine countries file for:\n{ambiguous_area_codes}."
    )
    assert len(ambiguous_area_codes) == 0, error
    n_area_codes_per_country = tb.groupby("country")["area_code"].transform("nunique")
    ambiguous_countries = (
        tb.loc[n_area_codes_per_country > 1][["area_code", "country"]]
        .drop_duplicates()
        .set_index("area_code")["country"]
        .to_dict()
    )
    error = (
        f"There cannot be multiple area codes for the same countries. "
        f"Redefine countries file for:\n{ambiguous_countries}."
    )
    assert len(ambiguous_countries) == 0, error


def check_that_regions_with_subregions_are_ignored_when_constructing_aggregates(
    countries_metadata: Table,
) -> None:
    """Check that regions that contain subregions are ignored when constructing region aggregates, to avoid
    double-counting those subregions.

    Parameters
    ----------
    countries_metadata : Table
        Table 'countries' from garden faostat_metadata dataset.

    """
    # Check if there is any harmonized regions that contain subregions.
    # If so, they should be ignored when constructing region aggregates, to avoid double-counting them.
    countries_with_subregions = (
        countries_metadata[
            (countries_metadata["country"] != "World")
            & (~countries_metadata["country"].isin(REGIONS_TO_ADD))
            & (~countries_metadata["country"].isin(REGIONS_TO_IGNORE_IN_AGGREGATES))
            & (~countries_metadata["country"].str.contains("(FAO)", regex=False).fillna(False))
            & (countries_metadata["members"].notnull())
        ]["country"]
        .unique()
        .tolist()
    )

    error = (
        f"Regions {countries_with_subregions} contain subregions. Add them to REGIONS_TO_IGNORE_IN_AGGREGATES to "
        f"avoid double-counting subregions when constructing aggregates."
    )
    assert len(countries_with_subregions) == 0, error


def harmonize_items(tb: Table, dataset_short_name: str, item_col: str = "item") -> Table:
    """Harmonize item codes (by ensuring they are strings of numbers with a fixed length, prepended with zeros), make
    amendments to faulty items, and make item codes and items of categorical dtype.

    Parameters
    ----------
    tb : Table
        Data before harmonizing item codes.
    dataset_short_name : str
        Dataset short name.
    item_col : str
        Name of items column.

    Returns
    -------
    tb : Table
        Data after harmonizing item codes.

    """
    tb = tb.copy()

    # Set the maximum number of characters for item_code.
    if dataset_short_name == f"{NAMESPACE}_sdgb":
        n_characters_item_code = N_CHARACTERS_ITEM_CODE_EXTENDED
    else:
        n_characters_item_code = N_CHARACTERS_ITEM_CODE

    # Note: Here list comprehension is faster than doing .astype(str).str.zfill(...).
    tb["item_code"] = [str(item_code).zfill(n_characters_item_code) for item_code in tb["item_code"]]

    # Convert both columns to category to reduce memory.
    tb = tb.astype({"item_code": "category", item_col: "category"})

    # Fix those few cases where there is more than one item per item code within a given dataset.
    if dataset_short_name in ITEM_AMENDMENTS:
        for amendment in ITEM_AMENDMENTS[dataset_short_name]:
            # Ensure new item code and item name are added as categories, to avoid errors.
            if amendment["new_item_code"] not in tb["item_code"].cat.categories:
                tb["item_code"] = tb["item_code"].cat.add_categories(amendment["new_item_code"])
            if amendment["new_fao_item"] not in tb[item_col].cat.categories:
                tb[item_col] = tb[item_col].cat.add_categories(amendment["new_fao_item"])

            # Update item code and item name.
            tb.loc[
                (tb["item_code"] == amendment["item_code"]) & (tb[item_col] == amendment["fao_item"]),
                ("item_code", item_col),
            ] = (amendment["new_item_code"], amendment["new_fao_item"])

    # Remove unused categories.
    tb["item_code"] = tb["item_code"].cat.remove_unused_categories()
    tb[item_col] = tb[item_col].cat.remove_unused_categories()

    return tb


def harmonize_elements(
    tb: Table, dataset_short_name: str, element_col: str = "element", unit_col: Optional[str] = "unit"
) -> Table:
    """Harmonize element codes (by ensuring they are strings of numbers with a fixed length, prepended with zeros), and
    make element codes and elements of categorical dtype.

    Parameters
    ----------
    tb : Table
        Data before harmonizing element codes.
    dataset_short_name : str
        Dataset short name.
    element_col : str
        Name of element column (this is only necessary to convert element column into categorical dtype).

    Returns
    -------
    tb : Table
        Data after harmonizing element codes.

    """
    tb = tb.copy()
    tb["element_code"] = [str(element_code).zfill(N_CHARACTERS_ELEMENT_CODE) for element_code in tb["element_code"]]

    # Convert both columns to category to reduce memory
    tb = tb.astype({"element_code": "category", element_col: "category"})

    # Fix those few cases where there is more than one item per item code within a given dataset.
    if dataset_short_name in ELEMENT_AMENDMENTS:
        for amendment in ELEMENT_AMENDMENTS[dataset_short_name]:
            # Ensure new item code and item name are added as categories, to avoid errors.
            if amendment["new_element_code"] not in tb["element_code"].cat.categories:
                tb["element_code"] = tb["element_code"].cat.add_categories(amendment["new_element_code"])
            if amendment["new_fao_element"] not in tb[element_col].cat.categories:
                tb[element_col] = tb[element_col].cat.add_categories(amendment["new_fao_element"])
            if unit_col is not None and amendment["new_fao_unit"] not in tb[unit_col].cat.categories:
                tb[unit_col] = tb[unit_col].cat.add_categories(amendment["new_fao_unit"])

            if unit_col is not None:
                # Update element code, element name, and unit name.
                tb.loc[
                    (tb["element_code"] == amendment["element_code"]) & (tb[element_col] == amendment["fao_element"]),
                    ("element_code", element_col, unit_col),
                ] = (amendment["new_element_code"], amendment["new_fao_element"], amendment["new_fao_unit"])
            else:
                # Update element code, and element name.
                tb.loc[
                    (tb["element_code"] == amendment["element_code"]) & (tb[element_col] == amendment["fao_element"]),
                    ("element_code", element_col),
                ] = (amendment["new_element_code"], amendment["new_fao_element"])

    return tb


def harmonize_countries(tb: Table, countries_metadata: Table) -> Table:
    """Harmonize country names.

    A new column 'country' will be added, with the harmonized country names. Column 'fao_country' will remain, to have
    the original FAO country name as a reference.

    Parameters
    ----------
    tb : Table
        Data before harmonizing country names.
    countries_metadata : Table
        Table 'countries' from garden faostat_metadata dataset.

    Returns
    -------
    tb : Table
        Data after harmonizing country names.

    """
    tb = tb.copy()
    # Add harmonized country names (from countries metadata) to data.
    tb = tb.merge(
        countries_metadata[["area_code", "fao_country", "country"]].rename(
            columns={"fao_country": "fao_country_check"}
        ),
        on="area_code",
        how="left",
    )

    # area_code should always be an int
    tb["area_code"] = tb["area_code"].astype(int)

    # Sanity check.
    country_mismatch = tb[(tb["fao_country"].astype(str) != tb["fao_country_check"])]
    if len(country_mismatch) > 0:
        faulty_mapping = country_mismatch.set_index("fao_country").to_dict()["fao_country_check"]
        log.warning(f"Mismatch between fao_country in data and in metadata: {faulty_mapping}")
    tb = tb.drop(columns="fao_country_check")

    # Remove unmapped countries.
    tb = tb[tb["country"].notnull()].reset_index(drop=True)

    # Further sanity checks.
    check_that_countries_are_well_defined(tb)
    check_that_regions_with_subregions_are_ignored_when_constructing_aggregates(countries_metadata)

    # Set appropriate dtypes.
    tb = tb.astype({"country": "category", "fao_country": "category"})

    return tb


def prepare_dataset_description(fao_description: str, owid_description: str) -> str:
    """Prepare dataset description using the original FAO description and an (optional) OWID description.

    Parameters
    ----------
    fao_description : str
        Original FAOSTAT dataset description.
    owid_description : str
        Optional OWID dataset description

    Returns
    -------
    description: str
        Dataset description.
    """

    description = ""
    if len(owid_description) > 0:
        description += owid_description + "\n\n"

    if len(fao_description) > 0:
        description += f"Original dataset description by FAOSTAT:\n{fao_description}"

    # Remove empty spaces at the beginning and end.
    description = description.strip()

    return description


def prepare_variable_description(item: str, element: str, item_description: str, element_description: str) -> str:
    """Prepare variable description by combining item and element names and descriptions.

    This will be used in the variable metadata of the wide table, and shown in grapher SOURCES tab.

    Parameters
    ----------
    item : str
        Item name.
    element : str
        Element name.
    item_description : str
        Item description.
    element_description : str
        Element description.

    Returns
    -------
    description : str
        Variable description.
    """
    description = f"Item: {item}\n"
    if len(item_description) > 0:
        description += f"Description: {item_description}\n"

    description += f"\nMetric: {element}\n"
    if len(element_description) > 0:
        description += f"Description: {element_description}"

    # Remove empty spaces at the beginning and end.
    description = description.strip()

    return description


def remove_rows_with_nan_value(tb: Table, verbose: bool = False) -> Table:
    """Remove rows for which column "value" is nan.

    Parameters
    ----------
    tb : Table
        Data for current dataset.
    verbose : bool
        True to display information about the number and fraction of rows removed.

    Returns
    -------
    tb : Table
        Data after removing nan values.

    """
    tb = tb.copy()
    # Number of rows with a nan in column "value".
    # We could also remove rows with any nan, however, before doing that, we would need to assign a value to nan flags.
    n_rows_with_nan_value = len(tb[tb["value"].isnull()])
    if n_rows_with_nan_value > 0:
        frac_nan_rows = n_rows_with_nan_value / len(tb)
        if verbose:
            log.info(f"Removing {n_rows_with_nan_value} rows ({frac_nan_rows: .2%}) " f"with nan in column 'value'.")
        if frac_nan_rows > 0.15:
            log.warning(f"{frac_nan_rows: .0%} rows of nan values removed.")
        tb = tb.dropna(subset="value").reset_index(drop=True)

    return tb


def remove_columns_with_only_nans(tb: Table, verbose: bool = True) -> Table:
    """Remove columns that only have nans.

    In principle, it should not be possible that columns have only nan values, but we use this function just in case.

    Parameters
    ----------
    tb : Table
        Data for current dataset.
    verbose : bool
        True to display information about the removal of columns with nan values.

    Returns
    -------
    tb : Table
        Data after removing columns of nans.

    """
    tb = tb.copy()
    # Remove columns that only have nans.
    columns_of_nans = tb.columns[tb.isnull().all(axis=0)]
    if len(columns_of_nans) > 0:
        if verbose:
            log.info(
                f"Removing {len(columns_of_nans)} columns ({len(columns_of_nans) / len(tb.columns): .2%}) "
                f"that have only nans."
            )
        tb = tb.drop(columns=columns_of_nans)

    return tb


def remove_duplicates(tb: Table, index_columns: List[str], verbose: bool = True) -> Table:
    """Remove rows with duplicated index (country, year, item, element, unit).

    First attempt to use flags to remove duplicates. If there are still duplicates, remove in whatever way possible.

    Parameters
    ----------
    tb : Table
        Data for current dataset.
    index_columns : list
        Columns expected to be used as index of the data.
    verbose : bool
        True to display a summary of the removed duplicates.

    Returns
    -------
    tb : Table
        Data (with a dummy numerical index) after removing duplicates.

    """
    tb = tb.copy()

    # Select columns that will be used as indexes.
    _index_columns = [column for column in index_columns if column in tb.columns]
    # Number of ambiguous indexes (those that have multiple data values).
    n_ambiguous_indexes = len(tb[tb.duplicated(subset=_index_columns, keep="first")])
    if n_ambiguous_indexes > 0:
        # Add flag ranking to dataset.
        flags_ranking = FLAGS_RANKING.copy()
        flags_ranking["flag"] = flags_ranking["flag"].fillna(FLAG_OFFICIAL_DATA)
        tb = tb.merge(
            flags_ranking[["flag", "ranking"]].rename(columns={"ranking": "flag_ranking"}),
            on="flag",
            how="left",
        ).astype({"flag": "category"})

        # Number of ambiguous indexes that cannot be solved using flags.
        n_ambiguous_indexes_unsolvable = len(tb[tb.duplicated(subset=_index_columns + ["flag_ranking"], keep="first")])
        # Remove ambiguous indexes (those that have multiple data values).
        # When possible, use flags to prioritise among duplicates.
        tb = tb.sort_values(_index_columns + ["flag_ranking"]).drop_duplicates(subset=_index_columns, keep="first")
        frac_ambiguous = n_ambiguous_indexes / len(tb)
        frac_ambiguous_solved_by_flags = 1 - (n_ambiguous_indexes_unsolvable / n_ambiguous_indexes)
        if verbose:
            log.info(
                f"Removing {n_ambiguous_indexes} ambiguous indexes ({frac_ambiguous: .2%}). "
                f"{frac_ambiguous_solved_by_flags: .2%} of ambiguities were solved with flags."
            )

        tb = tb.drop(columns=["flag_ranking"])

    return tb


def clean_year_column(year_column: Variable) -> Variable:
    """Clean year column.

    Year is given almost always as an integer value. But sometimes (e.g. in the faostat_fs dataset) it is a range of
    years (that differ by exactly 2 years, e.g. "2010-2012"). This function returns a series of integer years, which, in
    the cases where the original year was a range, corresponds to the mean of the range.

    Parameters
    ----------
    year_column : Variable
        Original column of year values (which may be integer, or ranges of values).

    Returns
    -------
    year_clean_series : Variable
        Clean column of years, as integer values.

    """
    year_clean = []
    for year in year_column:
        if "-" in str(year):
            year_range = year.split("-")
            year_min = int(year_range[0])
            year_max = int(year_range[1])
            assert year_max - year_min == 2
            year_clean.append(year_min + 1)
        else:
            year_clean.append(int(year))

    # Prepare series of integer year values.
    year_clean_series = Variable(year_clean, name="year")

    return year_clean_series


def add_custom_names_and_descriptions(tb: Table, items_metadata: Table, elements_metadata: Table) -> Table:
    """Add columns with custom names, descriptions and conversion factors for elements, items and units.

    The returned dataframe will have the same number of rows as the ingested data, but:
    * Column 'element' will become the customized OWID element name.
    * A new column 'fao_element' will be added, with the original FAO element name.
    * A new column 'element_description' will be added, with the customized OWID element description.
    * Column 'item' will become the customized OWID item name.
    * A new column 'fao_item' will be added, with the original FAO item name.
    * A new column 'item_description' will be added, with the customized OWID item description.
    * Column 'unit' will become the customized OWID unit name (long version).
    * A new column 'unit_short_name' will be added, with the customized OWID unit name (short version).
    * A new column 'fao_unit_short_name' will be added, with the original FAO unit name (short version).
    * A new column 'unit_factor' will be added, with the custom factor that values have to be multiplied by (which is
      not done by this function).

    NOTE:
    * Given that an item code can have different item names in different datasets, it is important that items_metadata
    argument contains only item codes only for the relevant domain. For example, if data comes from the faostat_qcl
    dataset, items_metadata should contain only items from that dataset. This can be achieved by selecting
    `items_metadata["dataset"] == 'faostat_qcl']` before passing it to this function.
    * The same applies to elements_metadata: For safety, it should only contain elements of the relevant domain.

    Parameters
    ----------
    tb : Table
        Data for a particular domain, with harmonized item codes and element codes.
    items_metadata : Table
        Table 'items' from the garden faostat_metadata dataset, after selecting items for the current dataset.
    elements_metadata : Table
        Table 'elements' from the garden faostat_metadata dataset, after selecting elements for the current dataset.

    Returns
    -------
    tb : Table
        Data after adding and editing its columns as described above.

    """
    tb = tb.copy()

    error = "There are missing item codes in metadata."
    assert set(tb["item_code"]) <= set(items_metadata["item_code"]), error

    error = "There are missing element codes in metadata."
    assert set(tb["element_code"]) <= set(elements_metadata["element_code"]), error

    _expected_n_rows = len(tb)
    tb = tb.rename(columns={"item": "fao_item"}, errors="raise").merge(
        items_metadata[["item_code", "owid_item", "owid_item_description"]],
        on="item_code",
        how="left",
    )
    assert len(tb) == _expected_n_rows, "Something went wrong when merging data with items metadata."

    tb = tb.rename(columns={"element": "fao_element", "unit": "fao_unit_short_name"}, errors="raise").merge(
        elements_metadata[
            [
                "element_code",
                "owid_element",
                "owid_unit",
                "owid_unit_factor",
                "owid_element_description",
                "owid_unit_short_name",
            ]
        ],
        on=["element_code"],
        how="left",
    )
    assert len(tb) == _expected_n_rows, "Something went wrong when merging data with elements metadata."

    # `category` type was lost during merge, convert it back
    tb = tb.astype(
        {
            "element_code": "category",
            "item_code": "category",
        }
    )

    # Remove "owid_" from column names.
    tb = tb.rename(columns={column: column.replace("owid_", "") for column in tb.columns})

    # Fill missing unit and short_unit columns with empty strings.
    for column in ["unit", "unit_short_name"]:
        missing_unit_mask = tb[column].isnull()
        if not tb[missing_unit_mask].empty:
            log.warning(f"Missing {column} for elements: {set(tb[missing_unit_mask]['element'])}")
            tb[column] = tb[column].cat.add_categories("").fillna("")

    return tb


def remove_regions_from_countries_regions_members(countries_regions: Table, regions_to_remove: List[str]) -> Table:
    """Remove regions that have to be ignored from the lists of members in the countries-regions dataset.

    Parameters
    ----------
    countries_regions : Table
        Countries-regions dataset (from the OWID catalog).
    regions_to_remove : list
        Regions to ignore.

    Returns
    -------
    countries_regions : Table
        Countries-regions dataset after removing regions from the lists of members of each country or region.

    """
    countries_regions = countries_regions.copy()
    countries_regions["members"] = countries_regions["members"].dropna().astype(str)

    # Get the owid code for each region that needs to be ignored when creating region aggregates.
    regions_to_ignore_codes = []
    for region in set(regions_to_remove):
        selected_region = countries_regions[countries_regions["name"] == region]
        assert len(selected_region) == 1, f"Region {region} ambiguous or not found in countries_regions dataset."
        regions_to_ignore_codes.append(selected_region.index[0])

    # Remove those regions to ignore from lists of members of each region.
    regions_mask = countries_regions["members"].notnull()
    countries_regions.loc[regions_mask, "members"] = [
        json.dumps(list(set(json.loads(members)) - set(regions_to_ignore_codes)))
        for members in countries_regions[regions_mask]["members"]
    ]

    return countries_regions


def load_population(ds_population: Dataset) -> Table:
    """Load OWID population dataset, and add historical regions to it.

    Returns
    -------
    population : Table
        Population dataset.

    """
    # Load population dataset.
    population = ds_population["population"].reset_index()[["country", "year", "population"]]

    # Add data for historical regions (if not in population) by adding the population of its current successors.
    countries_with_population = population["country"].unique()
    missing_countries = [country for country in HISTORIC_TO_CURRENT_REGION if country not in countries_with_population]
    for country in missing_countries:
        members = HISTORIC_TO_CURRENT_REGION[country]["members"]
        _population = (
            population[population["country"].isin(members)]
            .groupby("year")
            .agg({"population": "sum", "country": "nunique"})
            .reset_index()
        )
        # Select only years for which we have data for all member countries.
        _population = _population[_population["country"] == len(members)].reset_index(drop=True)
        _population["country"] = country
        population = pr.concat([population, _population], ignore_index=True).reset_index(drop=True)

    error = "Duplicate country-years found in population. Check if historical regions changed."
    assert population[population.duplicated(subset=["country", "year"])].empty, error

    return population


def remove_overlapping_data_between_historical_regions_and_successors(
    data_region: Table,
) -> Table:
    """Remove overlapping data between a historical region and any of its successors (if there is any overlap), to avoid
    double-counting those regions when aggregating data.

    Data for historical regions (e.g. USSR) could overlap with data of the successor countries (e.g. Russia). If this
    happens, remove data (on the overlapping element-item-years) of the historical country.

    Parameters
    ----------
    data_region : Table
        Data (after selecting the countries of a certain relevant region).

    Returns
    -------
    data_region : Table
        Data after removing data with overlapping regions.

    """
    data_region = data_region.copy()

    columns = ["item_code", "element_code", "year"]
    indexes_to_drop = []
    for historical_region in HISTORIC_TO_CURRENT_REGION:
        # Successors of the current historical region.
        historical_successors = HISTORIC_TO_CURRENT_REGION[historical_region]["members"]
        # Unique combinations of item codes, element codes, and years for which historical region has data.
        historical_region_years = data_region[(data_region["country"] == historical_region)][columns].drop_duplicates()
        # Unique combinations of item codes, element codes, and years for which successors have data.
        historical_successors_years = data_region[(data_region["country"].isin(historical_successors))][
            columns
        ].drop_duplicates()
        # Find unique years where the above combinations of item-element-years of region and successors overlap.
        overlapping_years = pr.concat([historical_region_years, historical_successors_years], ignore_index=True)
        overlapping_years = overlapping_years[overlapping_years.duplicated()]
        if not overlapping_years.empty:
            log.warning(
                f"Removing rows where historical region {historical_region} overlaps with its successors "
                f"(years {sorted(set(overlapping_years['year']))})."
            )
            # Select rows in data_region to drop.
            overlapping_years["country"] = historical_region
            indexes_to_drop.extend(
                data_region.reset_index()
                .merge(
                    overlapping_years,
                    how="inner",
                    on=["country"] + columns,
                )["index"]
                .tolist()
            )

    if len(indexes_to_drop) > 0:
        # Remove rows of data of the historical region where its data overlaps with data from its successors.
        data_region = data_region.drop(index=indexes_to_drop)

    return data_region


def add_regions(
    tb: Table,
    ds_regions: Dataset,
    ds_income_groups: Dataset,
    ds_population: Dataset,
    elements_metadata: Table,
) -> Table:
    """Add region aggregates (i.e. aggregate data for continents and income groups).

    Regions to be created are defined above, in REGIONS_TO_ADD, and the variables for which data will be aggregated are
    those that, in the custom_elements_and_units.csv file, have a non-empty 'owid_aggregation' field (usually with
    'sum', or 'mean'). The latter field determines the type of aggregation to create.

    Historical regions (if any) will be included in the aggregations, after ensuring that there is no overlap between
    the data for the region, and the data of any of its successor countries (for each item-element-year).

    Parameters
    ----------
    tb : Table
        Clean data (after harmonizing items, element and countries).
    elements_metadata : Table
        Table 'elements' from the garden faostat_metadata dataset, after selecting elements for the current domain.

    Returns
    -------
    tb_with_regions : Table
        Data after adding rows for aggregate regions.

    """
    tb_with_regions = tb.copy()

    # Create a dictionary of aggregations, specifying the operation to use when creating regions.
    # These aggregations are defined in the custom_elements_and_units.csv file, and added to the metadata dataset.
    aggregations = (
        elements_metadata[(elements_metadata["owid_aggregation"].notnull())]
        .set_index("element_code")
        .to_dict()["owid_aggregation"]
    )
    if len(aggregations) > 0:
        log.info("add_regions", shape=tb_with_regions.shape)

        # Load population dataset, countries-regions, and income groups datasets.
        population = load_population(ds_population=ds_population)

        # Invert dictionary of aggregations to have the aggregation as key, and the list of element codes as value.
        aggregations_inverted = {
            unique_value: pd.unique([item for item, value in aggregations.items() if value == unique_value]).tolist()
            for unique_value in aggregations.values()
        }
        for region in tqdm(REGIONS_TO_ADD, file=sys.stdout):
            countries_in_region = geo.list_members_of_region(
                region,
                ds_regions=ds_regions,
                ds_income_groups=ds_income_groups,
                excluded_regions=REGIONS_TO_IGNORE_IN_AGGREGATES,
                include_historical_regions_in_income_groups=True,
            )
            region_code = REGIONS_TO_ADD[region]["area_code"]
            region_population = population[population["country"] == region][["year", "population"]].reset_index(
                drop=True
            )
            region_min_frac_population_with_data = REGIONS_TO_ADD[region]["min_frac_population_with_data"]
            for aggregation in aggregations_inverted:
                # List of element codes for which the same aggregate method (e.g. "sum") will be applied.
                element_codes = aggregations_inverted[aggregation]

                # Select relevant rows in the data.
                data_region = tb_with_regions[
                    (tb_with_regions["country"].isin(countries_in_region))
                    & (tb_with_regions["element_code"].isin(element_codes))
                ]

                # Ensure there is no overlap between historical regions and their successors.
                data_region = remove_overlapping_data_between_historical_regions_and_successors(data_region)

                if len(data_region) > 0:
                    data_region = (
                        dataframes.groupby_agg(
                            df=data_region.dropna(subset="value"),
                            groupby_columns=[
                                "year",
                                "item_code",
                                "element_code",
                                "item",
                                "element",
                                "fao_element",
                                "fao_item",
                                "item_description",
                                "unit",
                                "unit_short_name",
                                "fao_unit_short_name",
                                "element_description",
                            ],
                            num_allowed_nans=None,
                            frac_allowed_nans=None,
                            aggregations={
                                "value": aggregation,
                                "flag": lambda x: x if len(x) == 1 else FLAG_MULTIPLE_FLAGS,
                                "population_with_data": "sum",
                            },
                        )
                        .reset_index()
                        .dropna(subset="element")
                    )

                    # Add total population of the region (for each year) to the relevant data.
                    data_region = data_region.merge(region_population, on="year", how="left")

                    # Keep only rows for which we have sufficient data.
                    data_region = data_region[
                        (data_region["population_with_data"] / data_region["population"])
                        >= region_min_frac_population_with_data
                    ].reset_index(drop=True)

                    # Add region's name and area code.
                    data_region["country"] = region
                    data_region["area_code"] = region_code

                    # Use category type which is more efficient than using strings
                    data_region = data_region.astype(
                        {
                            "flag": "category",
                            "country": "category",
                        }
                    )

                    # Add data for current region to data.
                    tb_with_regions = dataframes.concatenate(
                        [tb_with_regions[tb_with_regions["country"] != region].reset_index(drop=True), data_region],
                        ignore_index=True,
                    )

            # Check that the fraction of population with data is as high as expected.
            frac_population = tb_with_regions["population_with_data"] / tb_with_regions["population"]
            assert frac_population[frac_population.notnull()].min() >= region_min_frac_population_with_data

        # Drop column of total population (we will still keep population_with_data).
        tb_with_regions = tb_with_regions.drop(columns=["population"])

        # Make area_code of category type (it contains integers and strings, and feather does not support object types).
        tb_with_regions["area_code"] = tb_with_regions["area_code"].astype(str).astype("category")

        # Sort conveniently.
        tb_with_regions = tb_with_regions.sort_values(["country", "year"]).reset_index(drop=True)

        check_that_countries_are_well_defined(tb_with_regions)

    # Copy metadata of the original table (including indicators metadata).
    tb_with_regions = tb_with_regions.copy_metadata(from_table=tb)

    return tb_with_regions


def add_fao_population_if_given(tb: Table) -> Table:
    """Add a new column for FAO population, if population values are given in the data.

    Some datasets (e.g. faostat_fbsh and faostat_fbs) include per-capita variables from the beginning. When this
    happens, FAO population may be given as another item-element. To be able to convert those per-capita variables into
    total values, we need to extract that population data and make it a new column.

    Parameters
    ----------
    tb : Table
        Data (after harmonizing elements and items, but before harmonizing countries).

    Returns
    -------
    tb : Table
        Data, after adding a column 'fao_population', if FAO population was found in the data.

    """
    # Name of item and element of FAO population (used to select population in the data).
    fao_population_item_name = "Population"
    fao_population_element_name = "Total Population - Both sexes"
    # Expected name of unit of FAO population.
    fao_population_unit_name = "thousand Number"
    # Select rows that correspond to FAO population.
    population_rows_mask = (tb["fao_item"] == fao_population_item_name) & (
        tb["fao_element"] == fao_population_element_name
    )

    if population_rows_mask.any():
        tb = tb.copy()

        fao_population = tb[population_rows_mask].reset_index(drop=True)

        # Check that population is given in "1000 persons" and convert to persons.
        assert list(fao_population["unit"].unique()) == [
            fao_population_unit_name
        ], "FAO population may have changed units."
        fao_population["value"] *= 1000

        # Note: Here we will dismiss the flags related to population. But they are only relevant for those columns
        # that were given as per capita variables.
        fao_population = (
            fao_population[["area_code", "year", "value"]]
            .drop_duplicates()
            .dropna(how="any")
            .rename(columns={"value": "fao_population"})
        )

        # Add FAO population as a new column in data.
        tb = tb.merge(fao_population, how="left", on=["area_code", "year"])

    return tb


def add_population(
    tb: Table,
    ds_population: Dataset,
    country_col: str = "country",
    year_col: str = "year",
    population_col: str = "population",
    warn_on_missing_countries: bool = True,
    show_full_warning: bool = True,
) -> Table:
    """Add a column of OWID population to the countries in the data, including population of historical regions.

    This function has been adapted from datautils.geo, because population currently does not include historic regions.
    We include them in this function.

    Parameters
    ----------
    tb : Table
        Data without a column for population (after harmonizing elements, items and country names).
    country_col : str
        Name of country column in data.
    year_col : str
        Name of year column in data.
    population_col : str
        Name for new population column in data.
    warn_on_missing_countries : bool
        True to warn if population is not found for any of the countries in the data.
    show_full_warning : bool
        True to show affected countries if the previous warning is raised.

    Returns
    -------
    tb_with_population : Table
        Data after adding a column for population for all countries in the data.

    """

    # Load population dataset.
    population = load_population(ds_population=ds_population).rename(
        columns={
            "country": country_col,
            "year": year_col,
            "population": population_col,
        }
    )[[country_col, year_col, population_col]]

    # Check if there is any missing country.
    missing_countries = set(tb[country_col]) - set(population[country_col])
    if len(missing_countries) > 0:
        if warn_on_missing_countries:
            geo.warn_on_list_of_entities(
                list_of_entities=missing_countries,
                warning_message=(
                    f"{len(missing_countries)} countries not found in population"
                    " dataset. They will remain in the dataset, but have nan"
                    " population."
                ),
                show_list=show_full_warning,
            )

    # Add population to original dataframe.
    tb_with_population = tb.merge(population, on=[country_col, year_col], how="left")

    return tb_with_population


def convert_variables_given_per_capita_to_total_value(tb: Table, elements_metadata: Table) -> Table:
    """Replace variables given per capita in the original data by total values.

    NOTE:
    * Per-capita variables to be replaced by their total values are those with 'was_per_capita' equal to 1 in the
      custom_elements_and_units.csv file.
    * The new variables will have the same element codes as the original per-capita variables.

    Parameters
    ----------
    tb : Table
        Data (after harmonizing elements and items, but before harmonizing countries).
    elements_metadata : Table
        Table 'elements' from the garden faostat_metadata dataset, after selecting the elements of the relevant domain.

    Returns
    -------
    tb : Table
        Data, after converting per-capita variables to total value.

    """
    # Select element codes that were originally given as per capita variables (if any), and, if FAO population is
    # given, make them total variables instead of per capita.
    # All variables in the custom_elements_and_units.csv file with "was_per_capita" True will be converted into
    # total (non-per-capita) values.
    element_codes_that_were_per_capita = list(
        elements_metadata[elements_metadata["was_per_capita"]]["element_code"].unique()
    )
    if len(element_codes_that_were_per_capita) > 0:
        tb = tb.copy()

        assert "fao_population" in tb.columns, "fao_population not found, maybe it changed item, element."

        # Select variables that were given as per capita variables in the original data and that need to be converted.
        per_capita_mask = tb["element_code"].isin(element_codes_that_were_per_capita)

        # Multiply them by the FAO population to convert them into total value.
        tb.loc[per_capita_mask, "value"] = tb[per_capita_mask]["value"] * tb[per_capita_mask]["fao_population"]

        # Include an additional description to all elements that were converted from per capita to total variables.
        if "" not in tb["element_description"].cat.categories:
            tb["element_description"] = tb["element_description"].cat.add_categories([""])
        tb.loc[per_capita_mask, "element_description"] = tb.loc[per_capita_mask, "element_description"].fillna("")
        tb["element_description"] = dataframes.apply_on_categoricals(
            [tb.element_description, per_capita_mask.astype("category")],
            lambda desc, mask: f"{desc} {WAS_PER_CAPITA_ADDED_ELEMENT_DESCRIPTION}".lstrip() if mask else f"{desc}",
        )

    return tb


def add_per_capita_variables(tb: Table, elements_metadata: Table) -> Table:
    """Add per-capita variables to data in a long format (and keep original variables as well).

    NOTE:
    * Variables for which new per-capita rows will be created are those with 'make_per_capita' equal to 1 in the
      custom_elements_and_units.csv file.
    * The new variables will have the same element codes as the original per-capita variables, with 'pc' appended to
    the number.

    Parameters
    ----------
    tb : Table
        Clean data (after harmonizing item codes and element codes, and countries, and adding aggregate regions).
    elements_metadata : Table
        Elements table from the garden faostat_metadata dataset, after selecting elements for the relevant domain.

    Returns
    -------
    tb : Table
        Data with per-capita variables.

    """
    tb_with_pc_variables = tb.copy()

    # Find element codes that have to be made per capita.
    element_codes_to_make_per_capita = list(
        elements_metadata[elements_metadata["make_per_capita"]]["element_code"].unique()
    )
    if len(element_codes_to_make_per_capita) > 0:
        log.info("add_per_capita_variables", shape=tb_with_pc_variables.shape)

        # Create a new dataframe that will have all per capita variables.
        per_capita_data = tb_with_pc_variables[
            tb_with_pc_variables["element_code"].isin(element_codes_to_make_per_capita)
        ].reset_index(drop=True)

        # Change element codes of per capita variables.
        per_capita_data["element_code"] = per_capita_data["element_code"].cat.rename_categories(
            lambda c: (c.lstrip("0") + "pc").zfill(N_CHARACTERS_ELEMENT_CODE)
        )

        # Create a mask that selects FAO regions (regions that, in the countries.json file, were not harmonized, and
        # have '(FAO)' at the end of the name).
        fao_regions_mask = per_capita_data["country"].str.contains("(FAO)", regex=False)
        # Create a mask that selects all other regions (i.e. harmonized countries).
        owid_regions_mask = ~fao_regions_mask

        # Create per capita variables for FAO regions (this can only be done if a column for FAO population is given).
        if "fao_population" in per_capita_data.columns:
            per_capita_data.loc[fao_regions_mask, "value"] = (
                per_capita_data[fao_regions_mask]["value"] / per_capita_data[fao_regions_mask]["fao_population"]
            )
        else:
            # Per capita variables can't be created for FAO regions, since we don't have FAO population.
            # Remove these regions from the per capita dataframe; only OWID harmonized countries will be kept.
            per_capita_data = per_capita_data[~fao_regions_mask].reset_index(drop=True)
            owid_regions_mask = np.ones(len(per_capita_data), dtype=bool)

        # Add per capita values to all other regions that are not FAO regions.
        per_capita_data.loc[owid_regions_mask, "value"] = (
            per_capita_data[owid_regions_mask]["value"] / per_capita_data[owid_regions_mask]["population_with_data"]  # type: ignore
        )

        # Remove nans (which may have been created because of missing FAO population).
        per_capita_data = per_capita_data.dropna(subset="value").reset_index(drop=True)  # type: ignore

        # Add "per capita" to all units.
        per_capita_data["unit"] = per_capita_data["unit"].cat.rename_categories(lambda c: f"{c} per capita")
        # Include an additional note in the description on affected elements.
        per_capita_data["element_description"] = per_capita_data["element_description"].cat.rename_categories(
            lambda c: f"{c} {NEW_PER_CAPITA_ADDED_ELEMENT_DESCRIPTION}"
        )
        # Add new rows with per capita variables to data.
        tb_with_pc_variables = dataframes.concatenate(
            [tb_with_pc_variables, per_capita_data], ignore_index=True
        ).reset_index(drop=True)

    # Copy metadata of the original table (including indicators metadata).
    tb_with_pc_variables = tb_with_pc_variables.copy_metadata(from_table=tb)

    return tb_with_pc_variables


def clean_data_values(values: Variable, amendments: Dict[str, str]) -> Variable:
    """Fix spurious data values (defined in value_amendments.csv) and make values a float column.

    Parameters
    ----------
    values : Variable
        Content of the "value" column in the original data.

    Returns
    -------
    values_clean : Variable
        Original values after fixing known issues and converting to float.

    """
    values_clean = values.copy()
    if len(amendments) > 0:
        values_clean = Variable(
            dataframes.map_series(
                series=values_clean,
                mapping=amendments,
                warn_on_missing_mappings=False,
                warn_on_unused_mappings=True,
                show_full_warning=True,
            ),
            name="value",
        )

    # Convert all numbers into numeric.
    # Note: If this step fails with a ValueError, it may be because other spurious values have been introduced.
    # If so, add them to value_amendments.csv and re-run faostat_metadata.
    values_clean = values_clean.astype(float)

    return values_clean


def clean_data(
    tb: Table,
    ds_population: Dataset,
    items_metadata: Table,
    elements_metadata: Table,
    countries_metadata: Table,
    amendments: Dict[str, str],
) -> Table:
    """Process data (with already harmonized item codes and element codes), before adding aggregate regions and
    per-capita variables.

    NOTE:
    * Given that an item code can have different item names in different datasets, it is important that items_metadata
    argument contains only item codes only for the relevant domain. For example, if data comes from the faostat_qcl
    dataset, items_metadata should contain only items from that dataset. This can be achieved by selecting
    `items_metadata["dataset"] == 'faostat_qcl']` before passing it to this function.
    * The same applies to elements_metadata: For safety, it should only contain elements of the relevant domain.

    Parameters
    ----------
    tb : Table
        Unprocessed data for current dataset (with harmonized item codes and element codes).
    items_metadata : Table
        Items metadata (from the metadata dataset) after selecting items for only the relevant domain.
    elements_metadata : Table
        Elements metadata (from the metadata dataset) after selecting elements for only the relevant domain.
    countries_metadata : Table
        Countries metadata (from the metadata dataset).
    amendments : dict
        Value amendments (if any).

    Returns
    -------
    tb : Table
        Processed data, ready to be made into a table for a garden dataset.

    """
    tb = tb.copy()

    # Fix spurious data values (applying mapping in value_amendments.csv) and ensure column of values is float.
    tb["value"] = clean_data_values(tb["value"], amendments=amendments)

    # Convert nan flags into "official" (to avoid issues later on when dealing with flags).
    tb["flag"] = Variable(
        [flag if not pd.isnull(flag) else FLAG_OFFICIAL_DATA for flag in tb["flag"]],
        dtype="category",
        name="flag",
    )

    # Some datasets (at least faostat_fa) use "recipient_country" instead of "area". For consistency, change this.
    tb = tb.rename(
        columns={
            "area": "fao_country",
            "recipient_country": "fao_country",
            "recipient_country_code": "area_code",
        }
    )

    # Ensure year column is integer (sometimes it is given as a range of years, e.g. 2013-2015).
    tb["year"] = clean_year_column(tb["year"])

    # Remove rows with nan value.
    tb = remove_rows_with_nan_value(tb)

    if len(items_metadata) > 0 and len(elements_metadata) > 0:
        # This is not fulfilled for faostat_qv since the last update.
        # Use custom names for items, elements and units (and keep original names in "fao_*" columns).
        tb = add_custom_names_and_descriptions(tb, items_metadata, elements_metadata)

        # Multiply data values by their corresponding unit factor, if any was given, and then drop unit_factor column.
        unit_factor_mask = tb["unit_factor"].notnull()
        tb.loc[unit_factor_mask, "value"] = tb[unit_factor_mask]["value"] * tb[unit_factor_mask]["unit_factor"]
        tb = tb.drop(columns=["unit_factor"])

        # Add FAO population as an additional column (if given in the original data).
        tb = add_fao_population_if_given(tb)

    # Convert variables that were given per-capita to total value.
    tb = convert_variables_given_per_capita_to_total_value(tb, elements_metadata=elements_metadata)

    # Harmonize country names.
    tb = harmonize_countries(tb=tb, countries_metadata=countries_metadata)

    # Remove duplicated data points (if any) keeping the one with lowest ranking flag (i.e. highest priority).
    tb = remove_duplicates(
        tb=tb,
        index_columns=["area_code", "year", "item_code", "element_code"],
        verbose=True,
    )

    # Add column for population; when creating region aggregates, this column will have the population of the countries
    # for which there was data. For example, for Europe in a specific year, the population may differ from item to item,
    # because for one item we may have more European countries informed than for the other.
    tb = add_population(
        tb=tb, ds_population=ds_population, population_col="population_with_data", warn_on_missing_countries=False
    )

    # Convert back to categorical columns (maybe this should be handled automatically in `add_population_to_dataframe`)
    tb = tb.astype({"country": "category"})

    return tb


def optimize_table_dtypes(table: Table) -> Table:
    """Optimize the dtypes of the columns in a table.

    NOTE: Using `.astype` in a loop over different columns is slow. Instead, it is better to map all columns at once or
    call `repack_frame` with dtypes arg

    Parameters
    ----------
    table : Table
        Table with possibly non-optimal column dtypes.

    Returns
    -------
    optimized_table : Table
        Table with optimized dtypes.

    """
    dtypes = {c: "category" for c in ["area_code", "item_code", "element_code"] if c in table.columns}

    # Store variables metadata before optimizing table dtypes (otherwise they will be lost).
    variables_metadata = {variable: table[variable].metadata for variable in table.columns}

    optimized_table = repack.repack_frame(table, dtypes=dtypes)

    # Recover variable metadata (that was lost when optimizing table dtypes).
    for variable in variables_metadata:
        optimized_table[variable].metadata = variables_metadata[variable]

    return optimized_table


def prepare_long_table(tb: Table) -> Table:
    """Prepare a data table in long format.

    Parameters
    ----------
    tb : Table
        Data (as a dataframe) in long format.

    Returns
    -------
    tb_long : Table
        Data (as a table) in long format.

    """
    # Create new table with long data.
    tb_long = tb.copy()

    # Ensure table has the optimal dtypes before storing it as feather file.
    tb_long = optimize_table_dtypes(table=tb_long)

    # Set appropriate indexes.
    index_columns = ["area_code", "year", "item_code", "element_code"]
    tb_long = tb_long.set_index(index_columns, verify_integrity=True).sort_index()

    # Sanity check.
    number_of_infinities = len(tb_long[tb_long["value"] == np.inf])
    assert number_of_infinities == 0, f"There are {number_of_infinities} infinity values in the long table."

    return tb_long


def create_variable_short_names(variable_name: str) -> str:
    """Create lower-snake-case short names for the columns in the wide (flatten) output table, ensuring that they are
    not too long (to avoid issues when inserting variable in grapher).

    If a new name is too long, the ending of the item name will be reduced.
    If the item name is not long enough to solve the problem, this function will raise an assertion error.

    Parameters
    ----------
    variable_name : str
        Variable name.

    Returns
    -------
    new_name : str
        New variable name.

    """
    # Extract all the necessary fields from the variable name.
    item, item_code, element, element_code, unit = variable_name.replace("||", "|").split(" | ")

    # Check that the extraction was correct by constructing the variable name again and comparing with the original.
    assert variable_name == f"{item} | {item_code} || {element} | {element_code} || {unit}"

    new_name = underscore(variable_name)

    # Check that the number of characters of the short name is not too long.
    n_char = len(new_name)
    if n_char > 255:
        # This name will cause an issue when uploading to grapher (because of a limit of 255 characters in short name).
        # Remove the extra characters from the ending of the item name (if possible).
        n_char_to_be_removed = n_char - 255
        # It could happen that it is not the item name that is long, but the element name, dataset, or unit.
        # But for the moment, assume it is the item name.
        assert len(item) > n_char_to_be_removed, "Variable name is too long, but it is not due to item name."
        new_item = underscore(item)[0:-n_char_to_be_removed]
        new_name = underscore(f"{new_item} | {item_code} || {element} | {element_code} || {unit}")

    # Check that now the new name now fulfils the length requirement.
    error = "Variable short name is too long. Improve create_variable_names function to account for this case."
    assert len(new_name) <= 255, error

    return new_name


def prepare_wide_table(tb: Table) -> Table:
    """Flatten a long table to obtain a wide table with ["country", "year"] as index.

    The input table will be pivoted to have [country, year] as index, and as many columns as combinations of
    item-element-unit entities.

    Parameters
    ----------
    tb : Table
        Data for current domain.

    Returns
    -------
    tb_wide : Table
        Data table with index [country, year].

    """
    tb = tb.copy(deep=False)

    # Ensure "item" exists in data (there are some datasets where it may be missing).
    if "item" not in tb.columns:
        tb["item"] = ""

    # Construct a variable name that will not yield any possible duplicates.
    # This will be used as column names (which will then be formatted properly with underscores and lower case),
    # and also as the variable titles in grapher.
    # Also, for convenience, keep a similar structure as in the previous OWID dataset release.
    # Finally, ensure that the short name version of the variable is not too long
    # (which would cause issues when uploading to grapher).
    tb["variable_name"] = dataframes.apply_on_categoricals(
        [tb.item, tb.item_code, tb.element, tb.element_code, tb.unit],
        lambda item,
        item_code,
        element,
        element_code,
        unit: f"{item} | {item_code} || {element} | {element_code} || {unit}",
    )

    # Construct a human-readable variable display name (which will be shown in grapher charts).
    tb["variable_display_name"] = dataframes.apply_on_categoricals(
        [tb.item, tb.element, tb.unit],
        lambda item, element, unit: f"{item} - {element} ({unit})",
    )

    if "item_description" in tb.columns:
        # Construct a human-readable variable description (for the variable metadata).
        tb["variable_description"] = dataframes.apply_on_categoricals(
            [tb.item, tb.element, tb.item_description, tb.element_description],
            prepare_variable_description,
        )
    else:
        # This is the case for faostat_qv since the last update.
        tb["variable_description"] = ""

    # Pivot over long dataframe to generate a wide dataframe with country-year as index, and as many columns as
    # unique elements in "variable_name" (which should be as many as combinations of item-elements).
    # Note: We include area_code in the index for completeness, but by construction country-year should not have
    # duplicates.
    # Note: `pivot` operation is usually faster on categorical columns
    log.info("prepare_wide_table.pivot", shape=tb.shape)
    # Create a wide table with just the data values.
    tb_wide = tb.pivot(
        index=["area_code", "country", "year"],
        columns=["variable_name"],
        values="value",
    )

    # Add metadata to each new variable in the wide data table.
    log.info("prepare_wide_table.adding_metadata", shape=tb_wide.shape)

    # Add variable name.
    for column in tb_wide.columns:
        tb_wide[column].metadata.title = column

    # Add variable unit (long name).
    variable_name_mapping = _variable_name_map(tb, "unit")
    for column in tb_wide.columns:
        tb_wide[column].metadata.unit = variable_name_mapping[column]

    if "unit_short_name" in tb.columns:
        # Add variable unit (short name).
        variable_name_mapping = _variable_name_map(tb, "unit_short_name")
        for column in tb_wide.columns:
            tb_wide[column].metadata.short_unit = variable_name_mapping[column]
    else:
        # This is the case for faostat_qv since the last update.
        for column in tb_wide.columns:
            tb_wide[column].metadata.short_unit = ""

    # Add variable description.
    variable_name_mapping = _variable_name_map(tb, "variable_description")
    for column in tb_wide.columns:
        tb_wide[column].metadata.description = variable_name_mapping[column]

    # Add display and presentation parameters (for grapher).
    for column in tb_wide.columns:
        tb_wide[column].metadata.display = {}
        tb_wide[column].metadata.presentation = VariablePresentationMeta()

    # Display name.
    variable_name_mapping = _variable_name_map(tb, "variable_display_name")
    for column in tb_wide.columns:
        tb_wide[column].metadata.display["name"] = variable_name_mapping[column]
        tb_wide[column].metadata.presentation.title_public = variable_name_mapping[column]

    # Ensure columns have the optimal dtypes, but codes are categories.
    log.info("prepare_wide_table.optimize_table_dtypes", shape=tb_wide.shape)
    tb_wide = optimize_table_dtypes(table=tb_wide.reset_index())

    # Sort columns and rows conveniently.
    tb_wide = tb_wide.set_index(["country", "year"], verify_integrity=True)
    tb_wide = tb_wide[["area_code"] + sorted([column for column in tb_wide.columns if column != "area_code"])]
    tb_wide = tb_wide.sort_index(level=["country", "year"]).sort_index()

    # Make all column names snake_case.
    variable_to_short_name = {
        column: create_variable_short_names(variable_name=tb_wide[column].metadata.title)
        for column in tb_wide.columns
        if tb_wide[column].metadata.title is not None
    }
    tb_wide = tb_wide.rename(columns=variable_to_short_name, errors="raise")

    # Sanity check.
    number_of_infinities = np.isinf(tb_wide.select_dtypes(include=np.number).fillna(0)).values.sum()
    assert number_of_infinities == 0, f"There are {number_of_infinities} infinity values in the wide table."

    return tb_wide


def _variable_name_map(data: Table, column: str) -> Dict[str, str]:
    """Extract map {variable name -> column} from dataframe and make sure it is unique (i.e. ensure that one variable
    does not map to two distinct values)."""
    pivot = data.dropna(subset=[column]).groupby(["variable_name"], observed=True)[column].apply(set)
    assert all(pivot.map(len) == 1)
    return pivot.map(lambda x: list(x)[0]).to_dict()  # type: ignore


def parse_amendments_table(amendments: Table, dataset_short_name: str):
    amendments = Table(amendments).reset_index()
    # Create a dictionary mapping spurious values to amended values.
    amendments = (
        amendments[amendments["dataset"] == dataset_short_name]
        .drop(columns="dataset")
        .set_index("spurious_value")
        .to_dict()["new_value"]
    )
    # For some reason, empty values are loaded in the table as None. Change them to nan.
    amendments = {old: new if new is not None else np.nan for old, new in amendments.items()}

    return amendments


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Fetch the dataset short name from dest_dir.
    dataset_short_name = Path(dest_dir).name

    # Define path to current step file.
    current_step_file = (CURRENT_DIR / dataset_short_name).with_suffix(".py")

    # Get paths and naming conventions for current data step.
    paths = PathFinder(current_step_file.as_posix())

    # Load latest meadow dataset and read its main table.
    ds_meadow = paths.load_dataset(dataset_short_name)
    tb = ds_meadow[dataset_short_name].reset_index()

    # Load dataset of FAOSTAT metadata.
    metadata = paths.load_dataset(f"{NAMESPACE}_metadata")

    # Load dataset, items, element-units, countries metadata, and value amendments.
    dataset_metadata = metadata["datasets"].loc[dataset_short_name].to_dict()
    items_metadata = metadata["items"].reset_index()
    items_metadata = items_metadata[items_metadata["dataset"] == dataset_short_name].reset_index(drop=True)
    elements_metadata = metadata["elements"].reset_index()
    elements_metadata = elements_metadata[elements_metadata["dataset"] == dataset_short_name].reset_index(drop=True)
    countries_metadata = metadata["countries"].reset_index()
    amendments = parse_amendments_table(amendments=metadata["amendments"], dataset_short_name=dataset_short_name)

    # Load population dataset.
    ds_population = paths.load_dataset("population")

    # Load regions dataset.
    ds_regions = paths.load_dataset("regions")

    # Load income groups dataset.
    ds_income_groups = paths.load_dataset("income_groups")

    #
    # Process data.
    #
    # Harmonize items and elements, and clean data.
    tb = harmonize_items(tb=tb, dataset_short_name=dataset_short_name)
    tb = harmonize_elements(tb=tb, dataset_short_name=dataset_short_name)

    # Prepare data.
    tb = clean_data(
        tb=tb,
        ds_population=ds_population,
        items_metadata=items_metadata,
        elements_metadata=elements_metadata,
        countries_metadata=countries_metadata,
        amendments=amendments,
    )

    # Add data for aggregate regions.
    tb = add_regions(
        tb=tb,
        ds_regions=ds_regions,
        ds_population=ds_population,
        ds_income_groups=ds_income_groups,
        elements_metadata=elements_metadata,
    )

    # Add per-capita variables.
    tb = add_per_capita_variables(tb=tb, elements_metadata=elements_metadata)

    # Handle detected anomalies in the data.
    tb, anomaly_descriptions = handle_anomalies(dataset_short_name=dataset_short_name, tb=tb)

    # Create a long table (with item code and element code as part of the index).
    tb_long = prepare_long_table(tb=tb)

    # Create a wide table (with only country and year as index).
    tb_wide = prepare_wide_table(tb=tb)

    #
    # Save outputs.
    #
    # Update tables metadata.
    tb_long.metadata.short_name = dataset_short_name
    tb_long.metadata.title = dataset_metadata["owid_dataset_title"]
    tb_wide.metadata.short_name = f"{dataset_short_name}_flat"
    tb_wide.metadata.title = dataset_metadata["owid_dataset_title"] + ADDED_TITLE_TO_WIDE_TABLE

    # Initialise new garden dataset.
    ds_garden = create_dataset(dest_dir=dest_dir, tables=[tb_long, tb_wide], default_metadata=ds_meadow.metadata)
    # Update dataset metadata.
    # Add description of anomalies (if any) to the dataset description.
    ds_garden.metadata.description = dataset_metadata["owid_dataset_description"] + anomaly_descriptions
    ds_garden.metadata.title = dataset_metadata["owid_dataset_title"]

    # Create garden dataset.
    ds_garden.save()
