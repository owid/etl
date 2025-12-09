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
from owid.catalog import Dataset, Table, Variable, VariablePresentationMeta, warnings
from owid.catalog.utils import underscore
from owid.datautils import dataframes
from tqdm.auto import tqdm

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Initialise log.
log = structlog.get_logger()

# Define path to current folder and version of all datasets in this folder.
CURRENT_DIR = Path(__file__).parent
VERSION = CURRENT_DIR.name

# Name of FAOSTAT metadata dataset.
FAOSTAT_METADATA_SHORT_NAME = "faostat_metadata"

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
        {
            "item_code": "00002520",
            "fao_item": "Cereals, Other",
            "new_item_code": "00002520",
            "new_fao_item": "Cereals, other",
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
        {
            "element_code": "005911",
            "fao_element": "Export Quantity",
            "fao_unit": "1000 t",
            "new_element_code": "005911",
            "new_fao_element": "Export quantity",
            "new_fao_unit": "1000 t",
        },
        {
            "element_code": "005611",
            "fao_element": "Import Quantity",
            "fao_unit": "1000 t",
            "new_element_code": "005611",
            "new_fao_element": "Import quantity",
            "new_fao_unit": "1000 t",
        },
        {
            "element_code": "000645",
            "fao_element": "Food supply quantity (kg/capita/yr)",
            "fao_unit": "kg",
            "new_element_code": "000645",
            "new_fao_element": "Food supply quantity (kg/capita/yr)",
            "new_fao_unit": "kg/cap",
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
    # There have been multiple cases where "European Union (27)" directly from FAOSTAT was informed, while some of the member countries were not. This caused our aggregate of EU to have spurious jumps. Given that the definition of this region is identical to that of FAOSTAT, there's no need to create our own aggregate.
    # "European Union (27)": {
    #     "area_code": "OWID_EU27",
    #     "min_frac_population_with_data": MIN_FRAC_POPULATION_WITH_DATA,
    # },
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
WAS_PER_CAPITA_ADDED_ELEMENT_DESCRIPTION = "* This data was originally given per-capita by FAOSTAT. We converted this data into total figures by multiplying by FAOSTAT's population.\n"
# Additional explanation to append to element description for created per-capita variables.
NEW_PER_CAPITA_ADDED_ELEMENT_DESCRIPTION = (
    "* Per-capita values were obtained by dividing total figures by Our World in Data's population.\n"
    "* For regions defined by FAOSTAT, per-capita values were calculated using FAOSTAT's original population data, if available.\n"
)

# Additional text to include in the metadata title of the output wide table.
ADDED_TITLE_TO_WIDE_TABLE = " - Flattened table indexed by country-year."


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
    if dataset_short_name == "faostat_sdgb":
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

    # Sanity check.
    error = "Country names found in data, but not in countries_metadata."
    assert set(tb["fao_country"]) <= set(countries_metadata["fao_country"]), error

    # Add harmonized country names (from countries metadata) to data.
    tb = tb.merge(
        countries_metadata[["area_code", "fao_country", "country"]],
        on=["area_code", "fao_country"],
        how="left",
    )

    # area_code should always be an int
    tb["area_code"] = tb["area_code"].astype(int)

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


def prepare_description_from_producer(
    fao_item: str, fao_element: str, item_description: str, element_description: str
) -> str:
    """Prepare description from producer by combining the original item and element names and descriptions.

    This will be used in the variable metadata of the wide table.

    Parameters
    ----------
    fao_item : str
        Original FAOSTAT item name.
    fao_element : str
        Original FAOSTAT element name.
    item_description : str
        Item description.
    element_description : str
        Element description.

    Returns
    -------
    description : str
        Variable as described by FAOSTAT.

    """
    description = f"Item: {fao_item}\n\n"
    if len(item_description) > 0:
        description += f"Description: {item_description}\n"

    description += f"\nMetric: {fao_element}\n\n"
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
    missing_fields = {"fields": [], "elements": []}
    for column in ["unit", "unit_short_name"]:
        missing_unit_mask = tb[column].isnull()
        if not tb[missing_unit_mask].empty:
            tb[column] = tb[column].cat.add_categories("").fillna("")
            missing_fields["fields"].append(column)
            missing_fields["elements"] = sorted(set(missing_fields["elements"]) | set(tb[missing_unit_mask]["element"]))
    if missing_fields["fields"]:
        log.info(
            f"Filling missing fields {missing_fields['fields']} with ''. Affected elements: {missing_fields['elements']}"
        )

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
        if historical_region_years.empty and historical_successors_years.empty:
            overlapping_years = pd.DataFrame()
        else:
            overlapping_years = pr.concat([historical_region_years, historical_successors_years], ignore_index=True)
            overlapping_years = overlapping_years[overlapping_years.duplicated()]
        if not overlapping_years.empty:
            log.info(
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
            unique_value: pd.unique(
                np.array([item for item, value in aggregations.items() if value == unique_value])
            ).tolist()
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
        elements_metadata[elements_metadata["was_per_capita"] == 1]["element_code"].unique()
    )
    if len(element_codes_that_were_per_capita) > 0:
        tb = tb.copy()

        assert "fao_population" in tb.columns, "fao_population not found, maybe it changed item, element."

        # Select variables that were given as per capita variables in the original data and that need to be converted.
        per_capita_mask = tb["element_code"].isin(element_codes_that_were_per_capita)

        # Multiply them by the FAO population to convert them into total value.
        tb.loc[per_capita_mask, "value"] = tb[per_capita_mask]["value"] * tb[per_capita_mask]["fao_population"]

        # Include an additional description to all elements that were converted from per capita to total variables.
        tb.loc[per_capita_mask, "description_processing"] = tb.loc[per_capita_mask, "description_processing"].fillna("")
        tb.loc[per_capita_mask, "description_processing"] += WAS_PER_CAPITA_ADDED_ELEMENT_DESCRIPTION

    return tb


def add_per_capita_variables(tb: Table, elements_metadata: Table) -> Table:
    """Add per-capita variables to data in a long format (and keep original variables as well).

    NOTE:
    * Variables for which new per-capita rows will be created are those with 'make_per_capita' equal to 1 in the
      custom_elements_and_units.csv file.
    * The new variables will have the same element codes as the original per-capita variables, with 'pc' appended to
    the number.
    * For all OWID countries and regions, the data will be divided by OWID's population dataset. For "FAO" regions, the data will be divided by the FAO population, if it's given in the current dataset; if not given, those regions will not have per capita data.

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
        elements_metadata[elements_metadata["make_per_capita"] == 1]["element_code"].unique()
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
        per_capita_data.loc[owid_regions_mask, "value"] = per_capita_data[owid_regions_mask]["value"] / per_capita_data[
            owid_regions_mask
        ]["population_with_data"].astype(float)

        # Remove nans (which may have been created because of missing FAO population).
        per_capita_data = per_capita_data.dropna(subset="value").reset_index(drop=True)  # type: ignore

        # Add "per capita" to all units.
        per_capita_data["unit"] = per_capita_data["unit"].cat.rename_categories(lambda c: f"{c} per capita")
        # Include an additional note in the processing description on affected elements.
        per_capita_data["description_processing"] = per_capita_data["description_processing"].fillna("")
        per_capita_data["description_processing"] += NEW_PER_CAPITA_ADDED_ELEMENT_DESCRIPTION
        # Add new rows with per capita variables to data.
        tb_with_pc_variables = dataframes.concatenate(
            [tb_with_pc_variables, per_capita_data], ignore_index=True
        ).reset_index(drop=True)

    # Copy metadata of the original table (including indicators metadata).
    tb_with_pc_variables = tb_with_pc_variables.copy_metadata(from_table=tb)

    return tb_with_pc_variables


def add_modified_variables(tb: Table, dataset_short_name: str) -> Table:
    # For convenience, create additional indicators in different units.
    tb = tb.copy()
    # NOTE: A new (arbitrary) element code will be assigned (ending in "pe", as per capita edited), which is an edited version of the given per capita element code.
    additional_elements = {
        "faostat_fbsc": [
            {
                # Food supply per day in grams (currently, there is only supply per year in kg).
                "element_code_old": "0645pc",
                "element_code": "0645pe",
                "factor": 1000 / 365,
                "unit": "grams per day per capita",
                "unit_short_name": "g",
            },
            {
                # Imports per capita in kg (currently, there is only imports per capita in tonnes).
                "element_code_old": "5611pc",
                "element_code": "5611pe",
                "factor": 1000,
                "unit": "kilograms per capita",
                "unit_short_name": "kg",
            },
            {
                # Exports per capita in kg (currently, there is only exports per capita in tonnes).
                "element_code_old": "5911pc",
                "element_code": "5911pe",
                "factor": 1000,
                "unit": "kilograms per capita",
                "unit_short_name": "kg",
            },
            {
                # Domestic supply per capita in kg (currently, there is only domestic supply per capita in tonnes).
                "element_code_old": "5301pc",
                "element_code": "5301pe",
                "factor": 1000,
                "unit": "kilograms per capita",
                "unit_short_name": "kg",
            },
            {
                # Food per capita in kg (currently, there is only food per capita in tonnes).
                "element_code_old": "5142pc",
                "element_code": "5142pe",
                "factor": 1000,
                "unit": "kilograms per capita",
                "unit_short_name": "kg",
            },
            {
                # Feed per capita in kg (currently, there is only feed per capita in tonnes).
                "element_code_old": "5521pc",
                "element_code": "5521pe",
                "factor": 1000,
                "unit": "kilograms per capita",
                "unit_short_name": "kg",
            },
            {
                # Other uses per capita in kg (currently, there is only other uses per capita in tonnes).
                "element_code_old": "5154pc",
                "element_code": "5154pe",
                "factor": 1000,
                "unit": "kilograms per capita",
                "unit_short_name": "kg",
            },
            {
                # Waste per capita in kg (currently, there is only waste per capita in tonnes).
                "element_code_old": "5123pc",
                "element_code": "5123pe",
                "factor": 1000,
                "unit": "kilograms per capita",
                "unit_short_name": "kg",
            },
        ],
        "faostat_qcl": [
            {
                # Production per capita in kg (currently, there is only production per capita in tonnes).
                "element_code_old": "5510pc",
                "element_code": "5510pe",
                "factor": 1000,
                "unit": "kilograms per capita",
                "unit_short_name": "kg",
            },
            {
                # Area harvested per capita in m2 (currently, there is only area harvested per capita in hectares).
                "element_code_old": "5312pc",
                "element_code": "5312pe",
                "factor": 10000,
                "unit": "square meters per capita",
                "unit_short_name": "m²",
            },
        ],
    }

    for element in additional_elements.get(dataset_short_name, []):
        _tb = tb[tb["element_code"] == element["element_code_old"]].reset_index(drop=True)
        _tb["value"] *= element["factor"]
        _tb["unit"] = element["unit"]
        _tb["unit_short_name"] = element["unit_short_name"]
        _tb["element_code"] = element["element_code"]
        # Add new rows to the original table.
        tb = pr.concat(
            [tb, _tb.astype({"unit": "category", "unit_short_name": "category", "element_code": "category"})],
            ignore_index=True,
        )

    return tb


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
        ).copy_metadata(from_variable=values)

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

    # Ensure a description processing column exists.
    tb["description_processing"] = None

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

    # Convert back to categorical columns (maybe this should be handled automatically in `add_population_to_table`)
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
            [tb.fao_item, tb.fao_element, tb.item_description, tb.element_description],
            prepare_description_from_producer,
        )
    else:
        # This is the case for faostat_qv since the last update.
        tb["variable_description"] = ""

    # Pivot over long dataframe to generate a wide dataframe with country-year as index, and as many columns as
    # unique elements in "variable_name" (which should be as many as combinations of item-elements).
    # Note: `pivot` operation is usually faster on categorical columns
    log.info("prepare_wide_table.pivot", shape=tb.shape)
    # Create a wide table with just the data values.
    tb_wide = tb.pivot(
        index=["country", "year"],
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
        tb_wide[column].metadata.description_from_producer = variable_name_mapping[column]

    # Add display and presentation parameters (for grapher).
    variable_name_mapping = _variable_name_map(tb, "variable_display_name")
    for column in tb_wide.columns:
        tb_wide[column].metadata.display = {"name": variable_name_mapping[column]}
        tb_wide[column].metadata.presentation = VariablePresentationMeta(title_public=variable_name_mapping[column])

    # Add processing description.
    variable_name_mapping = _variable_name_map(tb, "description_processing", enforce_unique=False)
    for column in tb_wide.columns:
        tb_wide[column].metadata.description_processing = variable_name_mapping.get(column)

    # Ensure columns have the optimal dtypes, but codes are categories.
    log.info("prepare_wide_table.optimize_table_dtypes", shape=tb_wide.shape)
    tb_wide = optimize_table_dtypes(table=tb_wide.reset_index())

    # Sort columns and rows conveniently.
    tb_wide = tb_wide.format(sort_columns=True)

    # Make all column names snake_case.
    variable_to_short_name = {
        column: create_variable_short_names(variable_name=tb_wide[column].metadata.title)
        for column in tb_wide.columns
        if tb_wide[column].metadata.title is not None
    }
    tb_wide = tb_wide.rename(columns=variable_to_short_name, errors="raise")

    # Remove columns that only contain nans and zeros.
    with warnings.ignore_warnings():
        tb_wide = tb_wide.loc[:, ~(tb_wide.fillna(0).sum() == 0)]

    # Sanity check.
    number_of_infinities = np.isinf(tb_wide.select_dtypes(include=np.number).fillna(0)).values.sum()
    assert number_of_infinities == 0, f"There are {number_of_infinities} infinity values in the wide table."

    return tb_wide


def _variable_name_map(data: Table, column: str, enforce_unique: bool = True) -> Dict[str, str]:
    """Extract map {variable name -> column} from dataframe and make sure it is unique (i.e. ensure that one variable
    does not map to two distinct values)."""
    pivot = data.dropna(subset=[column]).groupby(["variable_name"], observed=True)[column].apply(set)
    if enforce_unique:
        assert all(pivot.map(len) == 1)
        return pivot.map(lambda x: list(x)[0]).to_dict()  # type: ignore
    else:
        return pivot.map(lambda x: "".join(dict.fromkeys(x))).to_dict()  # type: ignore


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


def sanity_check_custom_units(tb_wide: Table, ds_garden: Dataset) -> None:
    # Get units and short units from the original wide table.
    units_old = {c: tb_wide[c].m.unit for c in tb_wide.columns}
    short_units_old = {c: tb_wide[c].m.short_unit for c in tb_wide.columns}

    # Get new table (where metadata yaml has already been read and applied to each indicator)
    tb_new = ds_garden[[table_name for table_name in ds_garden.table_names if table_name.endswith("_flat")][0]]
    # Get units and short units from the new wide table.
    units_new = {c: tb_new[c].m.unit for c in tb_new.columns}
    short_units_old = {c: tb_new[c].m.short_unit for c in tb_new.columns}
    assert units_old.keys() == units_new.keys()
    assert short_units_old.keys() == short_units_old.keys()
    for column, unit_old in units_old.items():
        unit_new = units_new[column]
        short_unit_old = short_units_old[column]
        short_unit_new = short_units_old[column]
        common_message = "Consider adding this custom definition to the `custom_elements_and_units.csv` file, anre then re-running the garden `faostat_metadata` step."
        if unit_old != unit_new:
            log.warning(
                f"\nUnit changed after parsing the meta.yml file.\ncolumn: '{column}'\nold: '{unit_old}'\nnew: '{unit_new}'\n{common_message}"
            )
        if short_unit_old != short_unit_new:
            log.warning(
                f"\nShort unit changed after parsing the meta.yml file.\ncolumn: '{column}'\nold: '{short_unit_old}'\nnew: '{short_unit_new}'\n{common_message}"
            )


def improve_metadata(tb_wide: Table, dataset_short_name: str) -> None:
    # Improve metadata in wide table (this, unfortunately, cannot easily be achieved in the long table).
    # def prepare_public_titles(item: str, element: str, unit: str) -> str:

    ITEM_NAME_REPLACEMENTS = {
        "faostat_qcl": {
            # Meat items.
            "00001765": "All meat",  # From faostat_qcl - 'Meat, total' (previously 'Meat, total').
            "00001069": "Duck meat",  # From faostat_qcl - 'Meat of ducks, fresh or chilled' (previously 'Meat, duck').
            "00001806": "Beef and buffalo meat",  # From faostat_qcl - 'Meat, beef and buffalo' (previously 'Meat, beef and buffalo').
            "00001097": "Horse meat",  # From faostat_qcl - 'Horse meat, fresh or chilled' (previously 'Meat, horse').
            "00001808": "Poultry meat",  # From faostat_qcl - 'Meat, poultry' (previously 'Meat, poultry').
            "00000977": "Lamb and mutton meat",  # From faostat_qcl - 'Meat, lamb and mutton' (previously 'Meat, lamb and mutton').
            "00001127": "Camel meat",  # From faostat_qcl - 'Meat of camels, fresh or chilled' (previously 'Meat, camel').
            "00001080": "Turkey meat",  # From faostat_qcl - 'Meat of turkeys, fresh or chilled' (previously 'Meat, turkey').
            "00001108": "Donkey meat",  # From faostat_qcl - 'Meat of asses, fresh or chilled' (previously 'Meat, ass').
            "00001073": "Goose meat",  # From faostat_qcl - 'Meat of geese, fresh or chilled' (previously 'Meat, goose and guinea fowl').
            "00001035": "Pig meat",  # From faostat_qcl - 'Meat of pig with the bone, fresh or chilled' (previously 'Meat, pig').
            "00001163": "Game meat",  # From faostat_qcl - 'Game meat, fresh, chilled or frozen' (previously 'Meat, game').
            "00001807": "Sheep and goat meat",  # From faostat_qcl - 'Meat, sheep and goat' (previously 'Meat, sheep and goat').
            "00001141": "Rabbit and hare meat",  # From faostat_qcl - 'Meat of rabbits and hares, fresh or chilled' (previously 'Meat, rabbit').
            "00001058": "Chicken meat",  # From faostat_qcl - 'Meat of chickens, fresh or chilled' (previously 'Meat, chicken').
            "00000947": "Buffalo meat",  # From faostat_qcl - 'Meat of buffalo, fresh or chilled' (previously 'Meat, buffalo').
            "00001111": "Mule meat",  # From faostat_qcl - 'Meat of mules, fresh or chilled' (previously 'Meat, mule').
            "00001017": "Goat meat",  # From faostat_qcl - 'Meat of goat, fresh or chilled' (previously 'Meat, goat').
            "00000867": "Cattle meat with the bone",  # From faostat_qcl - "Meat of cattle with the bone, fresh or chilled".
            "00001158": "Other domestic camelid meat meat",  # From faostat_qcl - "Meat of other domestic camelids, fresh or chilled".
            "00001151": "Other domestic rodent meat",  # From faostat_qcl - "Meat of other domestic rodents, fresh or chilled".
            "00001089": "Pigeon and other bird meat",  # From faostat_qcl - "Meat of pigeons and other birds n.e.c., fresh, chilled or frozen".
            "00001166": "Other mammal meat",  # From faostat_qcl - "Other meat of mammals, fresh or chilled".
            # Fat.
            "00001019": "Unrendered goat fat",  # From faostat_qcl - 'Goat fat, unrendered' (previously 'Fat, goats').
            "00000869": "Unrendered cattle fat",  # From faostat_qcl - 'Cattle fat, unrendered' (previously 'Fat, cattle').
            "00001129": "Camel fat",  # From faostat_qcl - 'Fat of camels' (previously 'Fat, camels').
            "00000949": "Unrendered buffalo fat",  # From faostat_qcl - 'Buffalo fat, unrendered' (previously 'Fat, buffaloes').
            "00001037": "Pig fat",  # From faostat_qcl - 'Fat of pigs' (previously 'Fat, pigs').
            "00000979": "Unrendered sheep fat",  # From faostat_qcl - 'Sheep fat, unrendered' (previously 'Fat, sheep').
            "00001043": "Rendered pig fat",  # From faostat_qcl - "Pig fat, rendered".
            # Offals.
            "00000868": "Cattle offal",  # From faostat_qcl - 'Offals, cattle' (previously 'Offals, cattle').
            "00001098": "Horses and other equines offal",  # From faostat_qcl - 'Edible offals of horses and other equines,  fresh, chilled or frozen' (previously 'Offals, horses').
            "00000978": "Sheep offal",  # From faostat_qcl - 'Offals, sheep' (previously 'Offals, sheep').
            "00000948": "Buffalo offal",  # From faostat_qcl - 'Offals, buffaloes' (previously 'Offals, buffaloes').
            "00001128": "Camel offal",  # From faostat_qcl - 'Offals, camels' (previously 'Offals, camels').
            "00001036": "Pig offal",  # From faostat_qcl - 'Offals, pigs' (previously 'Offals, pigs').
            "00001018": "Goat offal",  # From faostat_qcl - 'Offals, goats' (previously 'Offals, goats').
            # Seeds.
            "00000289": "Sesame seeds",  # From faostat_qcl - 'Sesame seed' (previously 'Sesame seed').
            "00000267": "Sunflower seeds",  # From faostat_qcl - 'Sunflower seed' (previously 'Sunflower seed').
            "00000292": "Mustard seeds",  # From faostat_qcl - 'Mustard seed' (previously 'Mustard seed').
            "00000101": "Canary seeds",  # From faostat_qcl - 'Canary seed' (previously 'Canary seed').
            "00000280": "Safflower seeds",  # From faostat_qcl - 'Safflower seed' (previously 'Safflower seed').
            "00000328": "Unginned cotton seeds",  # From faostat_qcl - 'Seed cotton, unginned' (previously 'Seed cotton').
            "00000333": "Linseed",  # From faostat_qcl - 'Linseed' (previously 'Linseed').
            "00000336": "Hempseed",  # From faostat_qcl - 'Hempseed' (previously 'Hempseed').
            "00000329": "Cotton seed",  # From faostat_qcl - 'Cotton seed' (previously 'Cottonseed').
            "00000270": "Rape or colza seed",  # From faostat_qcl - 'Rape or colza seed' (previously 'Rapeseed').
            "00000299": "Melonseed",  # From faostat_qcl - 'Melonseed' (previously 'Melonseed').
            # Other.
            "00001107": "Donkeys",  # From faostat_qcl - 'Asses'.
            "00001034": "Pigs",  # From faostat_qcl - 'Swine / pigs'.
            "00001091": "Eggs (from other birds)",  # From faostat_qcl - 'Eggs from other birds (excl. hens)' (previously 'Eggs from other birds (excl. hens)').
            "00001062": "Eggs (from hens)",  # From faostat_qcl - 'Eggs from hens' (previously 'Eggs from hens').
            "00001783": "Eggs (from hens and other birds)",  # From faostat_qcl - 'Eggs' (previously 'Eggs Primary').
            "00000176": "Dry beans",  # From faostat_qcl - 'Beans, dry' (previously 'Beans, dry').
            "00000201": "Dry lentils",  # From faostat_qcl - 'Lentils, dry' (previously 'Lentils').
            "00000216": "Brazil nuts",  # From faostat_qcl - 'Brazil nuts, in shell' (previously 'Brazil nuts, with shell').
            "00001804": "Citrus fruit",  # From faostat_qcl - 'Citrus Fruit' (previously 'Citrus Fruit').
            "00000656": "Green coffee",  # From faostat_qcl - 'Coffee, green' (previously 'Coffee, green').
            "00000995": "Sheep skins",  # From faostat_qcl - 'Skins, sheep' (previously 'Skins, sheep').
            "00001025": "Goat skins",  # From faostat_qcl - 'Skins, goat' (previously 'Skins, goat').
            "00000771": "Raw or retted flax",  # From faostat_qcl - 'Flax, raw or retted' (previously 'Flax fibre').
            "00000220": "Chestnuts",  # From faostat_qcl - 'Chestnuts, in shell' (previously 'Chestnut').
            "00000417": "Green peas",  # From faostat_qcl - 'Peas, green' (previously 'Peas, green').
            "00001732": "Oilcrops (oil equivalent)",  # From faostat_qcl - 'Oilcrops, Oil Equivalent' (previously 'Oilcrops, Oil Equivalent').
            "00000223": "Pistachios",  # From faostat_qcl - 'Pistachios, in shell' (previously 'Pistachios').
            "00000187": "Dry peas",  # From faostat_qcl - 'Peas, dry' (previously 'Peas, dry').
            "00001841": "Oilcrops (cake equivalent)",  # From faostat_qcl - 'Oilcrops, Cake Equivalent' (previously 'Oilcrops, Cake Equivalent').
            "00000125": "Fresh cassava",  # From faostat_qcl - 'Cassava, fresh' (previously 'Cassava').
            "00000197": "Dry pigeon peas",  # From faostat_qcl - 'Pigeon peas, dry' (previously 'Pigeon peas').
            "00000249": "Coconuts",  # From faostat_qcl - 'Coconuts, in shell' (previously 'Coconuts').
            "00000780": "Raw or retted jute",  # From faostat_qcl - 'Jute, raw or retted' (previously 'Jute').
            "00000162": "Raw sugar",  # From faostat_qcl - 'Sugar (raw)' (previously 'Sugar (raw)').
            "00000056": "Maize (corn)",  # From faostat_qcl - 'Maize (corn)' (previously 'Maize').
            "00000414": "Green beans",  # From faostat_qcl - 'Other beans, green' (previously 'Beans, green').
            "00000592": "Kiwi",  # From faostat_qcl - 'Kiwi' (previously 'Kiwi').
            "00000800": "Other raw agave fibres",  # From faostat_qcl - "Agave fibres, raw, n.e.c.".
            "00000203": "Dry bambara beans",  # From faostat_qcl - "Bambara beans, dry".
            "00000051": "Malted beer of barley",  # From faostat_qcl - "Beer of barley, malted".
            "00000420": "Green broad and horse beans",  # From faostat_qcl - "Broad beans and horse beans, green".
            "00000899": "Dry buttermilk",  # From faostat_qcl - "Buttermilk, dry".
            "00000955": "Fresh or processed cheese from buffalo milk",  # From faostat_qcl - "Cheese from milk of buffalo, fresh or processed".
            "00001021": "Fresh or processed cheese from goat milk",  # From faostat_qcl - "Cheese from milk of goats, fresh or processed".
            "00000984": "Fresh or processed cheese from sheep milk",  # From faostat_qcl - "Cheese from milk of sheep, fresh or processed".
            "00000693": "Raw cinnamon and cinnamon tree flowers",  # From faostat_qcl - "Cinnamon and cinnamon-tree flowers, raw".
            "00000698": "Raw cloves (whole stems)",  # From faostat_qcl - "Cloves (whole stems), raw".
            "00000813": "Raw coir",  # From faostat_qcl - "Coir, raw".
            "00000885": "Fresh cream",  # From faostat_qcl - "Cream, fresh".
            "00017530": "Fibre crops (fibre equivalent)",  # From faostat_qcl - "Fibre Crops, Fibre Equivalent".
            "00000720": "Raw ginger",  # From faostat_qcl - "Ginger, raw".
            "00000778": "Raw kapok fibre",  # From faostat_qcl - "Kapok fibre, raw".
            "00000402": "Green onions and shallots",  # From faostat_qcl - "Onions and shallots, green".
            "00000512": "Other citrus fruit",  # From faostat_qcl - "Other citrus fruit, n.e.c.".
            "00000619": "Other fruits",  # From faostat_qcl - "Other fruits, n.e.c.".
            "00000339": "Other oil seeds",  # From faostat_qcl - "Other oil seeds, n.e.c.".
            "00000603": "Other tropical fruits",  # From faostat_qcl - "Other tropical fruits, n.e.c.".
            "00000463": "Other fresh vegetables",  # From faostat_qcl - "Other vegetables, fresh n.e.c.".
            "00000748": "Peppermint and spearmint",  # From faostat_qcl - "Peppermint, spearmint".
            "00000788": "Raw or retted ramie",  # From faostat_qcl - "Ramie, raw or retted".
            "00000789": "Raw sisal",  # From faostat_qcl - "Sisal, raw".
            "00001809": "Dry skim milk and buttermilk",  # From faostat_qcl - "Skim Milk & Buttermilk, Dry".
            "00000896": "Condensed skim milk",  # From faostat_qcl - "Skim milk, condensed".
            "00000895": "Evaporated skim milk",  # From faostat_qcl - "Skim milk, evaporated".
            "00000777": "Raw or retted true hemp",  # From faostat_qcl - "True hemp, raw or retted".
            "00000692": "Raw vanilla",  # From faostat_qcl - "Vanilla, raw".
            "00000890": "Condensed whey",  # From faostat_qcl - "Whey, condensed".
            "00000889": "Consdensed whole milk",  # From faostat_qcl - "Whole milk, condensed".
            "00000894": "Evaporated whole milk",  # From faostat_qcl - "Whole milk, evaporated".
            # Other odd cases (they would need to be manually fixed, I'll leave them for now).
            # "00000809": "abaca manila hemp raw",  # From faostat_qcl - "Abaca, manila hemp, raw".
            # "00000839": "balata gutta percha guayule chicle and similar natural gums in primary forms or in plates sheets or strip",  # From faostat_qcl - "Balata, gutta-percha, guayule, chicle and similar natural gums in primary forms or in plates, sheets or strip".
            # "00000401": "chillies and peppers green capsicum spp and pimenta spp",  # From faostat_qcl - "Chillies and peppers, green (Capsicum spp. and Pimenta spp.)".
            # "00000149": "edible roots and tubers with high starch or inulin content n e c fresh",  # From faostat_qcl - "Edible roots and tubers with high starch or inulin content, n.e.c., fresh".
            # "00000675": "green tea not fermented black tea fermented and partly fermented tea in immediate packings of a content not exceeding 3 kg",  # From faostat_qcl - "Green tea (not fermented), black tea (fermented) and partly fermented tea, in immediate packings of a content not exceeding 3 kg".
            # "00000782": "kenaf and other textile bast fibres raw or retted",  # From faostat_qcl - "Kenaf, and other textile bast fibres, raw or retted".
            # "00000702": "nutmeg mace cardamoms raw",  # From faostat_qcl - "Nutmeg, mace, cardamoms, raw".
            # "00000234": "other nuts excluding wild edible nuts and groundnuts in shell n e c",  # From faostat_qcl - "Other nuts (excluding wild edible nuts and groundnuts), in shell, n.e.c.".
            # "00000723": "other stimulant spice and aromatic crops n e c",  # From faostat_qcl - "Other stimulant, spice and aromatic crops, n.e.c.".
            # "00000394": "pumpkins squash and gourds",  # From faostat_qcl - "Pumpkins, squash and gourds".
            # "00000754": "pyrethrum dried flowers",  # From faostat_qcl - "Pyrethrum, dried flowers".
            # "00001176": "snails fresh chilled frozen dried salted or in brine except sea snails",  # From faostat_qcl - "Snails, fresh, chilled, frozen, dried, salted or in brine, except sea snails".
        },
        "faostat_fbsc": {
            # Meat.
            "00002943": "All meat",  # From faostat_fbsc - 'Meat, total' (previously 'Meat, total').
            "00002734": "Poultry meat",  # From faostat_fbsc - 'Meat, poultry' (previously 'Meat, poultry').
            "00002731": "Beef and buffalo meat",  # From faostat_fbsc - 'Bovine meat'.
            "00002732": "Sheep and goat meat",  # From faostat_fbsc - 'Meat, sheep and goat' (previously 'Meat, sheep and goat').
            "00002733": "Pig meat",  # From faostat_fbsc - 'Pork' (previously 'Pork').
            "00002768": "Aquatic mammals meat",  # From faostat_fbsc - "Meat, Aquatic Mammals".
            # Seeds.
            "00002557": "Sunflower seeds",  # From faostat_fbsc - 'Sunflower seed' (previously 'Sunflower seed').
            "00002561": "Sesame seeds",  # From faostat_fbsc - 'Sesame seed' (previously 'Sesame seed').
            "00002559": "Cottonseed",  # From faostat_fbsc - 'Cottonseed' (previously 'Cottonseed').
            # Other.
            "00002901": "All food",  # From faostat_fbsc - 'Total' (previously 'Total').
            "00002737": "Raw animal fats",  # From faostat_fbsc - 'Animal fats' (previously 'Animal fats').
            "00002946": "Animal fats",  # From faostat_fbsc - 'Animal fats group'.
            "00002546": "Dry beans",  # From faostat_fbsc - 'Beans, dry' (previously 'Beans, dry').
            "00002514": "Corn",  # From faostat_fbsc - 'Maize' (previously 'Maize').
            "00002547": "Dry peas",  # From faostat_fbsc - 'Peas, dry' (previously 'Peas, dry').
            "00002769": "Other aquatic animals",  # From faostat_fbsc - 'Aquatic animals, other'.
            "00002961": "Other aquatic products",  # From faostat_fbsc - 'Aquatic products, other'.
            "00002657": "Fermented beverages",  # From faostat_fbsc - 'Beverages, fermented'.
            "00002520": "Other cereals",  # From faostat_fbsc - 'Cereals, other'.
            "00002659": "Non-consumable alcohol",  # From faostat_fbsc - "Alcohol, Non-Food".
            "00002614": "Other citrus",  # From faostat_fbsc - "Citrus, Other".
            "00002781": "Fish body oil",  # From faostat_fbsc - "Fish, Body Oil".
            "00002782": "Fish liver oil",  # From faostat_fbsc - "Fish, Liver Oil".
            "00002625": "Other fruits",  # From faostat_fbsc - "Fruits, Other".
            "00002764": "Other marine fish",  # From faostat_fbsc - "Marine Fish, Other".
            "00002735": "Other meat",  # From faostat_fbsc - "Meat, Other".
            "00002767": "Other molluscs",  # From faostat_fbsc - "Molluscs, Other".
            "00002570": "Other oilcrops",  # From faostat_fbsc - "Oilcrops, Other".
            "00002534": "Other roots",  # From faostat_fbsc - "Roots, Other".
            "00002645": "Other spices",  # From faostat_fbsc - "Spices, Other".
            "00002543": "Other sweeteners",  # From faostat_fbsc - "Sweeteners, Other".
            "00002605": "Other vegetables",  # From faostat_fbsc - "Vegetables, Other".
            "00002586": "Other oilcrop oils",  # From faostat_fbsc - "Oilcrops Oil, Other".
            "00002549": "Other pulses and products",  # From faostat_fbsc - "Pulses, Other and products".
            # NOTE: It's unclear what the difference is between the following two, but for now, they are not used in the explorer.
            "00002924": "All alcoholic beverages",  # From faostat_fbsc - 'Alcoholic Beverages'.
            "00002658": "Alcoholic beverages",  # From faostat_fbsc - 'Beverages, alcoholic'.
        },
    }

    for column in tb_wide.columns:
        item, item_code, element, element_code, unit = sum(
            [[j.strip() for j in i.split("|")] for i in tb_wide[column].metadata.title.split("||")], []
        )

        # Replace item names in special cases.
        if dataset_short_name in ITEM_NAME_REPLACEMENTS:
            item = ITEM_NAME_REPLACEMENTS[dataset_short_name].get(item_code, item)

        # Ensure items don't have arbitrary capital letters.
        item = item.capitalize()

        # First define default metadata values:
        display_name = f"{item} - {element} ({unit})"
        title = display_name
        description_short = None
        num_decimal_places = 2

        # A few additional definitions.
        description_short_food_available = "Quantity that is available for consumption at the end of the supply chain. It does not account for consumer waste, so the quantity that is actually consumed may be lower."

        # Now redefine the title for special cases:
        if dataset_short_name == "faostat_fbsc":
            if element_code == "0645pc":
                # "0645pc",  # Food available for consumption (kilograms per year per capita)
                assert unit == "kilograms per year per capita"
                title = f"Yearly per capita supply of {item.lower()}"
                description_short = description_short_food_available
            elif element_code == "0645pe":
                # "0645pe",  # Food available for consumption (grams per day per capita) - created in the garden faostat_fbsc step.
                assert unit == "grams per day per capita"
                title = f"Daily per capita supply of {item.lower()}"
                description_short = description_short_food_available
            elif element_code == "0664pc":
                # "0664pc",  # Food available for consumption (kilocalories per day per capita)
                assert unit == "kilocalories per day per capita"
                title = f"Daily per capita supply of calories from {item.lower()}"
                description_short = description_short_food_available
            elif element_code == "0674pc":
                # "0674pc",  # Food available for consumption (grams of protein per day per capita)
                assert unit == "grams of protein per day per capita"
                title = f"Daily per capita supply of proteins from {item.lower()}"
                description_short = description_short_food_available
            elif element_code == "0684pc":
                # "0684pc",  # Food available for consumption (grams of fat per day per capita)
                assert unit == "grams of fat per day per capita"
                title = f"Daily per capita supply of fat from {item.lower()}"
                description_short = description_short_food_available
            elif element_code == "005142":
                # "005142",  # Food (tonnes)
                assert unit == "tonnes"
                title = f"{item} used for direct human food"
                description_short = "Quantity that is allocated for direct consumption as human food, rather than allocation to animal feed or industrial uses."
            elif element_code == "5142pc":
                # "5142pc",  # Food (tonnes per capita)
                assert unit == "tonnes per capita"
                title = f"{item} used for direct human food per capita"
                description_short = "Quantity that is allocated for direct consumption as human food, rather than allocation to animal feed or industrial uses."
            elif element_code == "5142pe":
                # "5142pe",  # Food (kilograms per capita)
                assert unit == "kilograms per capita"
                title = f"{item} used for direct human food per capita"
                description_short = "Quantity that is allocated for direct consumption as human food, rather than allocation to animal feed or industrial uses."
            elif element_code == "005521":
                # "005521",  # Feed (tonnes)
                assert unit == "tonnes"
                title = f"{item} used for animal feed"
            elif element_code == "5521pc":
                # "5521pc",  # Feed (tonnes per capita)
                assert unit == "tonnes per capita"
                title = f"{item} used for animal feed per capita"
                description_short = "Quantity allocated to feed livestock."
            elif element_code == "5521pe":
                # "5521pe",  # Feed (kilograms per capita)
                assert unit == "kilograms per capita"
                title = f"{item} used for animal feed per capita"
                description_short = "Quantity allocated to feed livestock."
            elif element_code == "005154":
                # "005154",  # Other uses (non-food) (tonnes)
                assert unit == "tonnes"
                title = f"{item} allocated to other uses"
                description_short = "Quantity allocated to industrial uses such as biofuel, pharmaceuticals or textile products, as well as other non-food uses like pet food."
            elif element_code == "5154pc":
                # "5154pc",  # Other uses (tonnes per capita)
                assert unit == "tonnes per capita"
                title = f"{item} allocated to other uses per capita"
                description_short = "Quantity allocated to industrial uses such as biofuel, pharmaceuticals or textile products, as well as other non-food uses like pet food."
            elif element_code == "5154pe":
                # "5154pe",  # Other uses (kilograms per capita)
                assert unit == "kilograms per capita"
                title = f"{item} allocated to other uses per capita"
                description_short = "Quantity allocated to industrial uses such as biofuel, pharmaceuticals or textile products, as well as other non-food uses like pet food."
            elif element_code == "005123":
                # "005123",  # Waste in supply chain (tonnes)
                assert unit == "tonnes"
                title = f"{item} wasted in supply chains"
                description_short = "Quantity that is lost or wasted in supply chains through poor handling, spoiling, lack of refrigeration and damage from the field to retail. It does not include consumer waste."
            elif element_code == "5123pc":
                # "5123pc",  # Waste in supply chain (tonnes per capita)
                assert unit == "tonnes per capita"
                title = f"{item} wasted in supply chains per capita"
                description_short = "Quantity that is lost or wasted in supply chains through poor handling, spoiling, lack of refrigeration and damage from the field to retail. It does not include consumer waste."
            elif element_code == "5123pe":
                # "5123pe",  # Waste in supply chain (kilograms per capita)
                assert unit == "kilograms per capita"
                title = f"{item} wasted in supply chains per capita"
                description_short = "Quantity that is lost or wasted in supply chains through poor handling, spoiling, lack of refrigeration and damage from the field to retail. It does not include consumer waste."
            elif element_code == "005301":
                # "005301",  # Domestic supply (tonnes)
                assert unit == "tonnes"
                title = f"Domestic supply of {item.lower()}"
                description_short = "Quantity of a commodity available for use within a country after accounting for trade and stock changes. It is calculated as production plus imports, minus exports, and adjusted for changes in stocks."
            elif element_code == "5301pc":
                # "5301pc",  # Domestic supply (tonnes per capita)
                assert unit == "tonnes per capita"
                title = f"Per capita domestic supply of {item.lower()}"
                description_short = "Quantity of a commodity available for use within a country after accounting for trade and stock changes. It is calculated as production plus imports, minus exports, and adjusted for changes in stocks."
            elif element_code == "5301pe":
                # "5301pe",  # Domestic supply (kilograms per capita)
                assert unit == "kilograms per capita"
                title = f"Per capita domestic supply of {item.lower()}"
            elif element_code == "005611":
                # "005611",  # Imports (tonnes)
                assert unit == "tonnes"
                title = f"Imports of {item.lower()}"
            elif element_code == "5611pc":
                # "5611pc",  # Imports (tonnes per capita)
                assert unit == "tonnes per capita"
                title = f"Per capita imports of {item.lower()}"
            elif element_code == "5611pe":
                # "5611pe",  # Imports (kilograms per capita)
                assert unit == "kilograms per capita"
                title = f"Per capita imports of {item.lower()}"
            elif element_code == "005911":
                # "005911",  # Exports (tonnes)
                assert unit == "tonnes"
                title = f"Exports of {item.lower()}"
            elif element_code == "5911pc":
                # "5911pc",  # Exports (tonnes per capita)
                assert unit == "tonnes per capita"
                title = f"Per capita exports of {item.lower()}"
            elif element_code == "5911pe":
                # "5911pe",  # Exports (kilograms per capita)
                assert unit == "kilograms per capita"
                title = f"Per capita exports of {item.lower()}"
            elif element_code == "005131":
                # "005131",  # Processing (tonnes)
                assert unit == "tonnes"
                title = f"Processing of {item.lower()}"
        elif dataset_short_name == "faostat_qcl":
            if element_code == "005510":
                # "005510",  # Production (tonnes).
                assert unit == "tonnes"
                title = f"Production of {item.lower()}"
                num_decimal_places = 0
            elif element_code == "5510pc":
                # "5510pc",  # Production per capita (tonnes per capita).
                assert unit == "tonnes per capita"
                title = f"Per capita production of {item.lower()}"
            elif element_code == "5510pe":
                # "5510pe",  # Production per capita (kilograms per capita).
                assert unit == "kilograms per capita"
                title = f"Per capita production of {item.lower()}"
            elif element_code == "005412":
                # "005412",  # Yield (tonnes per hectare).
                assert unit == "tonnes per hectare"
                title = f"Yield of {item.lower()}"
                description_short = (
                    "Yield is the amount produced per unit of land used, measured in tonnes per hectare."
                )
            elif element_code in ["005417", "005424"]:
                # "005417",  # Yield (kilograms per animal).
                # "005424",  # Yield (kilograms per animal).
                assert unit == "kilograms per animal"
                title = f"Production of {item.lower()} per animal"
            elif element_code == "005312":
                # "005312",  # Area harvested (hectares).
                assert unit == "hectares"
                title = f"Land used to produce {item.lower()}"
            elif element_code == "5312pc":
                # "5312pc",  # Area harvested per capita (hectares per capita).
                title = f"Per capita land used to produce {item.lower()}"
            elif element_code == "5312pe":
                # "5312pe",  # Area harvested per capita (square meteres per capita).
                title = f"Per capita land used to produce {item.lower()}"
            elif element_code in ["005320", "005321"]:
                # "005320",  # Producing or slaughtered animals (animals).
                # "005321",  # Producing or slaughtered animals (animals).
                assert unit == "animals"
                title = f"Animals slaughtered to produce {item.lower()}"
                num_decimal_places = 0
            elif element_code in ["5320pc", "5321pc"]:
                # "5320pc",  # Producing or slaughtered animals per capita (animals per capita).
                # "5321pc",  # Producing or slaughtered animals per capita (animals per capita).
                assert unit == "animals per capita"
                title = f"Animals slaughtered per capita to produce {item.lower()}"
            elif element_code == "005413":
                # "005413",  # Eggs per bird (eggs per bird).
                # NOTE: There are only two items for this element: eggs from hens and eggs from other birds.
                assert unit == "eggs per bird"
                if item == "00001062":
                    title = "Number of eggs per hen"
                elif item == "00001091":
                    title = "Number of eggs per bird (excluding hens)"
            elif element_code == "005313":
                # "005313",  # Laying (animals).
                assert unit == "animals"
                title = f"Laying animals to produce {item.lower()}"
            elif element_code == "005513":
                # "005513",  # Eggs produced (eggs).
                assert unit == "eggs"
                # NOTE: The only items are eggs from hens and eggs from other birds.
                title = f"Number of {item.lower()} produced"
            elif element_code == "005318":
                # "005318",  # Milk animals (animals).
                assert unit == "animals"
                title = f"Number of animals used to produce {item.lower()}"
            elif element_code == "005111":
                # "005111",  # Stocks (animals)
                assert unit == "animals"
                title = f"Live {item.lower()}"
                num_decimal_places = 0

        # Override titles in special cases:
        if item_code == "00002901":
            if element_code == "0664pc":
                assert unit == "kilocalories per day per capita"
                title = "Total daily supply of calories per person"
            elif element_code == "0674pc":
                assert unit == "grams of protein per day per capita"
                title = "Total daily supply of protein per person"
            elif element_code == "0684pc":
                assert unit == "grams of fat per day per capita"
                title = "Total daily supply of fat per person"

        # Add a text to the short description of some items that require further explanation.
        description_short_by_item_code = {
            "00001841": "Oilseed cake is the residue from oil extraction, commonly used as animal feed.",
            "00001732": "Oil equivalent is a measurement of oil extracted from oil-bearing crops.",
            "00000226": "The areca nut is the seed of the areca palm, and is commonly referred to as betel nut.",
            "00001717": "Cereals include wheat, rice, maize, barley, oats, rye, millet, sorghum, buckwheat, and mixed grains.",
            "00000656": "Green coffee beans are coffee seeds (beans) that have not yet been roasted.",
            "00001780": "Milk represents the raw equivalents of all dairy products including cheese, yoghurt, cream and milk consumed as the final product.",
            "00002551": "Nuts is the sum of all nut crops including brazil nuts, cashews, almonds, walnuts, pistachios, and areca nuts.",
            "00002911": "Pulses are the edible seeds of plants in the legume family.",
            "00001720": "Roots and tubers are a category of crops including cassava, potatoes, sweet potatoes, yams, and yautia.",
            "00000162": "Raw sugar is the total quantity of sugar product yielded from sugar cane and sugar beet crops, expressed in its raw equivalents.",
            "00001723": "Sugar crops is the sum of sugar cane and sugar beet.",
            "00002901": "This is the total of all agricultural produce, both crops and livestock.",
        }
        if item_code in description_short_by_item_code:
            description_short = description_short + " " if description_short else ""
            description_short += description_short_by_item_code[item_code]

        if dataset_short_name == "faostat_fbsc":
            # Add footnote to mention the change in methodology between FBSH and FBS, which often causes abrupt jumps in 2010.
            tb_wide[column].metadata.presentation.grapher_config = {
                "note": "FAOSTAT applies a methodological change from the year 2010 onwards."
            }

        # Update metadata.
        tb_wide[column].display["name"] = display_name
        tb_wide[column].display["numDecimalPlaces"] = num_decimal_places
        tb_wide[column].metadata.presentation.title_public = title
        tb_wide[column].metadata.description_short = description_short

        # Remove duplicate lines in processing description.
        processing = tb_wide[column].metadata.description_processing
        if processing is not None:
            tb_wide[column].metadata.description_processing = "\n".join(list(dict.fromkeys(processing.split("\n"))))


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
    metadata = paths.load_dataset("faostat_metadata")

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

    # For convenience, create additional indicators in different units.
    tb = add_modified_variables(tb=tb, dataset_short_name=dataset_short_name)

    # Create a long table (with item code and element code as part of the index).
    tb_long = prepare_long_table(tb=tb)

    # Create a wide table (with only country and year as index).
    tb_wide = prepare_wide_table(tb=tb)

    # Improve metadata (of wide table).
    improve_metadata(tb_wide=tb_wide, dataset_short_name=dataset_short_name)

    # Check that column "value" has an origin (other columns are not as important and may not have origins).
    error = f"Column 'value' of the long table of {dataset_short_name} must have one origin."
    assert len(tb_long["value"].metadata.origins) == 1, error
    error = f"All value columns of the wide table of {dataset_short_name} must have one origin."
    assert all([len(tb_wide[column].metadata.origins) == 1 for column in tb_wide.columns]), error

    #
    # Save outputs.
    #
    # Update tables metadata.
    tb_long.metadata.short_name = dataset_short_name
    tb_long.metadata.title = dataset_metadata["owid_dataset_title"]
    tb_wide.metadata.short_name = f"{dataset_short_name}_flat"
    tb_wide.metadata.title = dataset_metadata["owid_dataset_title"] + ADDED_TITLE_TO_WIDE_TABLE

    # Initialise new garden dataset.
    ds_garden = paths.create_dataset(
        tables=[tb_long, tb_wide],
        default_metadata=ds_meadow.metadata,
        check_variables_metadata=False,
    )

    # Sanity check custom units.
    sanity_check_custom_units(tb_wide=tb_wide, ds_garden=ds_garden)

    # Update dataset metadata.
    # The following description is not publicly shown in charts; it is only visible when accessing the catalog.
    ds_garden.metadata.description = dataset_metadata["owid_dataset_description"] + anomaly_descriptions

    # Create garden dataset.
    ds_garden.save()
