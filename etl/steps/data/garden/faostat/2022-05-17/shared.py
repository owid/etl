"""Shared definitions in FAOSTAT garden steps.

This module contains:
* Common functions used in garden steps.
* Definitions related to elements and items (e.g. item amendments).
* Definitions related to countries and regions (e.g. aggregate regions to generate and definition of historic regions).
* Definitions of flags (found in the original FAOSTAT data) and their ranking (i.e. the priority of data points when
  there are duplicates).
* Identified outliers.
* Other additional definitions (e.g. texts to include in the definitions of generated per-capita variables).

"""

import itertools
import json
import sys
from copy import deepcopy
from pathlib import Path
from typing import Dict, List, Union, cast

import numpy as np
import pandas as pd
import structlog
from owid import catalog, repack  # type: ignore
from owid.datautils import dataframes
from tqdm.auto import tqdm

from etl.data_helpers import geo
from etl.paths import DATA_DIR, STEP_DIR

# Initialise log.
log = structlog.get_logger()

# Namespace and version that will be assumed in all garden steps.
NAMESPACE = Path(__file__).parent.parent.name
VERSION = Path(__file__).parent.name

# Path to file containing information of the latest versions of the relevant datasets.
LATEST_VERSIONS_FILE = STEP_DIR / "data" / "garden" / NAMESPACE / VERSION / "versions.csv"

# Elements and items.

# Maximum number of characters for item_code.
# FAOSTAT "item_code" is usually an integer number, however sometimes it has decimals and sometimes it contains letters.
# So we will convert it into a string of this number of characters (integers will be prepended with zeros).
N_CHARACTERS_ITEM_CODE = 8
# Maximum number of characters for element_code (integers will be prepended with zeros).
N_CHARACTERS_ELEMENT_CODE = 6
# Manual fixes to item codes to avoid ambiguities.
ITEM_AMENDMENTS = {
    "faostat_sdgb": [
        {
            "item_code": "AG_PRD_FIESMSN_",
            "fao_item": "2.1.2 Population in moderate or severe food insecurity (thousands of people) (female)",
            "new_item_code": "AG_PRD_FIESMSN_FEMALE",
            "new_fao_item": "2.1.2 Population in moderate or severe food insecurity (thousands of people) (female)",
        },
        {
            "item_code": "AG_PRD_FIESMSN_",
            "fao_item": "2.1.2 Population in moderate or severe food insecurity (thousands of people) (male)",
            "new_item_code": "AG_PRD_FIESMSN_MALE",
            "new_fao_item": "2.1.2 Population in moderate or severe food insecurity (thousands of people) (male)",
        },
    ],
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
REGIONS_TO_IGNORE_IN_AGGREGATES = [
    "Melanesia",
    "Polynesia",
]

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
    "Eritrea and Ethiopia": {
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
    pd.DataFrame.from_records(
        columns=["flag", "description"],
        data=[
            # FAO uses nan flag for official data; in our datasets we will replace nans by FLAG_OFFICIAL_DATA.
            (np.nan, "Official data"),
            ("S", "Standardized data"),
            ("X", "International reliable sources"),
            (
                "W",
                "Data reported on country official publications or web sites (Official) or trade country files",
            ),
            ("Q", "Official data reported on FAO Questionnaires from countries"),
            (
                "Qm",
                "Official data from questionnaires and/or national sources and/or COMTRADE (reporters)",
            ),
            ("E", "Expert sources from FAO (including other divisions)"),
            (
                "I",
                "Country data reported by International Organizations where the country is a member (Semi-official) "
                "- WTO, EU, UNSD, etc.",
            ),
            (
                "A",
                "Aggregate, may include official, semi-official, estimated or calculated data",
            ),
            ("_A", "Normal value"),
            ("P", "Provisional official data"),
            ("_P", "Provisional value"),
            ("Fb", "Data obtained as a balance"),
            ("Fk", "Calculated data on the basis of official figures"),
            ("FC", "Calculated data"),
            ("Fc", "Calculated data"),
            ("Cv", "Calculated through value"),
            ("F", "FAO estimate"),
            ("_E", "Estimated value"),
            ("R", "Estimated data using trading partners database"),
            ("Fm", "Manual Estimation"),
            ("*", "Unofficial figure"),
            ("Im", "FAO data based on imputation methodology"),
            ("_I", "Imputed value (CCSA definition)"),
            (
                "Z",
                "When the Fertilizer Utilization Account (FUA) does not balance due to utilization from stockpiles, "
                "apparent consumption has been set to zero",
            ),
            ("SD", "Statistical Discrepancy"),
            ("Bk", "Break in series"),
            ("NR", "Not reported"),
            ("M", "Data not available"),
            ("NV", "Data not available"),
            ("_O", "Missing value"),
            ("_V", "Unvalidated value"),
            ("B", "Unknown flag"),
            ("_L", "Unknown flag"),
            ("_M", "Unknown flag"),
            ("_U", "Unknown flag"),
            ("w", "Unknown flag"),
            # The definition of flag "_" exists, but it's empty.
            ("_", ""),
        ],
    )
    .reset_index()
    .rename(columns={"index": "ranking"})
)


# Amendments to apply to data values.
# They will only be applied if "value" column is of type "category".
VALUE_AMENDMENTS = {
    # Replace values given as upper bounds (e.g. "<0.1") by the average between 0 and that value.
    "<0.1": "0.05",
    "<2.5": "1.25",
    "<0.5": "0.25",
    # Remove spurious comma values (this could be done simply by .str.replace(",", ""), however, given that it happens
    # only in a few cases, it seems safer to explicitly correct them here).
    "1,173.92": "1173.92",
    "1,688.37": "1688.37",
    "1,439.14": "1439.14",
    "1,248.25": "1248.25",
    "1,775.32": "1775.32",
    # Replace missing values by nan.
    "N": np.nan,
}

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


# Shared functions.


def check_that_countries_are_well_defined(data: pd.DataFrame) -> None:
    """Apply sanity checks related to the definition of countries.

    Parameters
    ----------
    data : pd.DataFrame
        Data, right after harmonizing country names.

    """
    # Ensure area codes and countries are well defined, and no ambiguities were introduced when mapping country names.
    n_countries_per_area_code = data.groupby("area_code")["country"].transform("nunique")
    ambiguous_area_codes = (
        data.loc[n_countries_per_area_code > 1][["area_code", "country"]]
        .drop_duplicates()
        .set_index("area_code")["country"]
        .to_dict()
    )
    error = (
        f"There cannot be multiple countries for the same area code. "
        f"Redefine countries file for:\n{ambiguous_area_codes}."
    )
    assert len(ambiguous_area_codes) == 0, error
    n_area_codes_per_country = data.groupby("country")["area_code"].transform("nunique")
    ambiguous_countries = (
        data.loc[n_area_codes_per_country > 1][["area_code", "country"]]
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
    countries_metadata: pd.DataFrame,
) -> None:
    """Check that regions that contain subregions are ignored when constructing region aggregates, to avoid
    double-counting those subregions.

    Parameters
    ----------
    countries_metadata : pd.DataFrame
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


def harmonize_items(df: pd.DataFrame, dataset_short_name: str, item_col: str = "item") -> pd.DataFrame:
    """Harmonize item codes (by ensuring they are strings of numbers with a fixed length, prepended with zeros), make
    amendments to faulty items, and make item codes and items of categorical dtype.

    Parameters
    ----------
    df : pd.DataFrame
        Data before harmonizing item codes.
    dataset_short_name : str
        Dataset short name.
    item_col : str
        Name of items column.

    Returns
    -------
    df : pd.DataFrame
        Data after harmonizing item codes.

    """
    df = df.copy()
    # Note: Here list comprehension is faster than doing .astype(str).str.zfill(...).
    df["item_code"] = [str(item_code).zfill(N_CHARACTERS_ITEM_CODE) for item_code in df["item_code"]]
    df[item_col] = df[item_col].astype(str)

    # Fix those few cases where there is more than one item per item code within a given dataset.
    if dataset_short_name in ITEM_AMENDMENTS:
        for amendment in ITEM_AMENDMENTS[dataset_short_name]:
            df.loc[
                (df["item_code"] == amendment["item_code"]) & (df[item_col] == amendment["fao_item"]),
                ("item_code", item_col),
            ] = (amendment["new_item_code"], amendment["new_fao_item"])

    # Convert both columns to category to reduce memory
    df = df.astype({"item_code": "category", item_col: "category"})

    return df


def harmonize_elements(df: pd.DataFrame, element_col: str = "element") -> pd.DataFrame:
    """Harmonize element codes (by ensuring they are strings of numbers with a fixed length, prepended with zeros), and
    make element codes and elements of categorical dtype.

    Parameters
    ----------
    df : pd.DataFrame
    element_col : str
        Name of element column (this is only necessary to convert element column into categorical dtype).

    Returns
    -------
    df : pd.DataFrame
        Data after harmonizing element codes.

    """
    df = df.copy()
    df["element_code"] = [str(element_code).zfill(N_CHARACTERS_ELEMENT_CODE) for element_code in df["element_code"]]

    # Convert both columns to category to reduce memory
    df = df.astype({"element_code": "category", element_col: "category"})

    return df


def harmonize_countries(data: pd.DataFrame, countries_metadata: pd.DataFrame) -> pd.DataFrame:
    """Harmonize country names.

    A new column 'country' will be added, with the harmonized country names. Column 'fao_country' will remain, to have
    the original FAO country name as a reference.

    Parameters
    ----------
    data : pd.DataFrame
        Data before harmonizing country names.
    countries_metadata : pd.DataFrame
        Table 'countries' from garden faostat_metadata dataset.

    Returns
    -------
    data : pd.DataFrame
        Data after harmonizing country names.

    """
    data = data.copy()
    if data["area_code"].dtype == "float64":
        # This happens at least for faostat_sdgb, where area code is totally different to the usual one.
        # See further explanations in garden step for faostat_metadata.
        # When this happens, merge using the old country name instead of the area code.
        data = pd.merge(
            data.rename(columns={"area_code": "m49_code"}),
            countries_metadata[["area_code", "m49_code", "fao_country", "country"]].rename(
                columns={"fao_country": "fao_country_check"}
            ),
            on="m49_code",
            how="left",
        ).drop(columns="m49_code")
    else:
        # Add harmonized country names (from countries metadata) to data.
        data = pd.merge(
            data,
            countries_metadata[["area_code", "fao_country", "country"]].rename(
                columns={"fao_country": "fao_country_check"}
            ),
            on="area_code",
            how="left",
        )

    # area_code should always be an int
    data["area_code"] = data["area_code"].astype(int)

    # Sanity check.
    error = "Mismatch between fao_country in data and in metadata."
    assert (data["fao_country"].astype(str) == data["fao_country_check"]).all(), error
    data = data.drop(columns="fao_country_check")

    # Remove unmapped countries.
    data = data[data["country"].notnull()].reset_index(drop=True)

    # Further sanity checks.
    check_that_countries_are_well_defined(data)
    check_that_regions_with_subregions_are_ignored_when_constructing_aggregates(countries_metadata)

    # Set appropriate dtypes.
    data = data.astype({"country": "category", "fao_country": "category"})

    return data


def remove_rows_with_nan_value(data: pd.DataFrame, verbose: bool = False) -> pd.DataFrame:
    """Remove rows for which column "value" is nan.

    Parameters
    ----------
    data : pd.DataFrame
        Data for current dataset.
    verbose : bool
        True to display information about the number and fraction of rows removed.

    Returns
    -------
    data : pd.DataFrame
        Data after removing nan values.

    """
    data = data.copy()
    # Number of rows with a nan in column "value".
    # We could also remove rows with any nan, however, before doing that, we would need to assign a value to nan flags.
    n_rows_with_nan_value = len(data[data["value"].isnull()])
    if n_rows_with_nan_value > 0:
        frac_nan_rows = n_rows_with_nan_value / len(data)
        if verbose:
            log.info(f"Removing {n_rows_with_nan_value} rows ({frac_nan_rows: .2%}) " f"with nan in column 'value'.")
        if frac_nan_rows > 0.15:
            log.warning(f"{frac_nan_rows: .0%} rows of nan values removed.")
        data = data.dropna(subset="value").reset_index(drop=True)

    return data


def remove_columns_with_only_nans(data: pd.DataFrame, verbose: bool = True) -> pd.DataFrame:
    """Remove columns that only have nans.

    In principle, it should not be possible that columns have only nan values, but we use this function just in case.

    Parameters
    ----------
    data : pd.DataFrame
        Data for current dataset.
    verbose : bool
        True to display information about the removal of columns with nan values.

    Returns
    -------
    data : pd.DataFrame
        Data after removing columns of nans.

    """
    data = data.copy()
    # Remove columns that only have nans.
    columns_of_nans = data.columns[data.isnull().all(axis=0)]
    if len(columns_of_nans) > 0:
        if verbose:
            log.info(
                f"Removing {len(columns_of_nans)} columns ({len(columns_of_nans) / len(data.columns): .2%}) "
                f"that have only nans."
            )
        data = data.drop(columns=columns_of_nans)

    return data


def remove_duplicates(data: pd.DataFrame, index_columns: List[str], verbose: bool = True) -> pd.DataFrame:
    """Remove rows with duplicated index (country, year, item, element, unit).

    First attempt to use flags to remove duplicates. If there are still duplicates, remove in whatever way possible.

    Parameters
    ----------
    data : pd.DataFrame
        Data for current dataset.
    index_columns : list
        Columns expected to be used as index of the data.
    verbose : bool
        True to display a summary of the removed duplicates.

    Returns
    -------
    data : pd.DataFrame
        Data (with a dummy numerical index) after removing duplicates.

    """
    data = data.copy()

    # Select columns that will be used as indexes.
    _index_columns = [column for column in index_columns if column in data.columns]
    # Number of ambiguous indexes (those that have multiple data values).
    n_ambiguous_indexes = len(data[data.duplicated(subset=_index_columns, keep="first")])
    if n_ambiguous_indexes > 0:
        # Add flag ranking to dataset.
        flags_ranking = FLAGS_RANKING.copy()
        flags_ranking["flag"] = flags_ranking["flag"].fillna(FLAG_OFFICIAL_DATA)
        data = pd.merge(
            data,
            flags_ranking[["flag", "ranking"]].rename(columns={"ranking": "flag_ranking"}),
            on="flag",
            how="left",
        ).astype({"flag": "category"})

        # Number of ambiguous indexes that cannot be solved using flags.
        n_ambiguous_indexes_unsolvable = len(
            data[data.duplicated(subset=_index_columns + ["flag_ranking"], keep="first")]
        )
        # Remove ambiguous indexes (those that have multiple data values).
        # When possible, use flags to prioritise among duplicates.
        data = data.sort_values(_index_columns + ["flag_ranking"]).drop_duplicates(subset=_index_columns, keep="first")
        frac_ambiguous = n_ambiguous_indexes / len(data)
        frac_ambiguous_solved_by_flags = 1 - (n_ambiguous_indexes_unsolvable / n_ambiguous_indexes)
        if verbose:
            log.info(
                f"Removing {n_ambiguous_indexes} ambiguous indexes ({frac_ambiguous: .2%}). "
                f"{frac_ambiguous_solved_by_flags: .2%} of ambiguities were solved with flags."
            )

        data = data.drop(columns=["flag_ranking"])

    return data


def clean_year_column(year_column: pd.Series) -> pd.Series:
    """Clean year column.

    Year is given almost always as an integer value. But sometimes (e.g. in the faostat_fs dataset) it is a range of
    years (that differ by exactly 2 years, e.g. "2010-2012"). This function returns a series of integer years, which, in
    the cases where the original year was a range, corresponds to the mean of the range.

    Parameters
    ----------
    year_column : pd.Series
        Original column of year values (which may be integer, or ranges of values).

    Returns
    -------
    year_clean_series : pd.Series
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
    year_clean_series = pd.Series(year_clean)
    year_clean_series.name = "year"

    return year_clean_series


def add_custom_names_and_descriptions(
    data: pd.DataFrame, items_metadata: pd.DataFrame, elements_metadata: pd.DataFrame
) -> pd.DataFrame:
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
    data : pd.DataFrame
        Data for a particular domain, with harmonized item codes and element codes.
    items_metadata : pd.DataFrame
        Table 'items' from the garden faostat_metadata dataset, after selecting items for the current dataset.
    elements_metadata : pd.DataFrame
        Table 'elements' from the garden faostat_metadata dataset, after selecting elements for the current dataset.

    Returns
    -------
    data : pd.DataFrame
        Data after adding and editing its columns as described above.

    """
    data = data.copy()

    error = "There are missing item codes in metadata."
    assert set(data["item_code"]) <= set(items_metadata["item_code"]), error

    error = "There are missing element codes in metadata."
    assert set(data["element_code"]) <= set(elements_metadata["element_code"]), error

    _expected_n_rows = len(data)
    data = pd.merge(
        data.rename(columns={"item": "fao_item"}),
        items_metadata[["item_code", "owid_item", "owid_item_description"]],
        on="item_code",
        how="left",
    )
    assert len(data) == _expected_n_rows, "Something went wrong when merging data with items metadata."

    data = pd.merge(
        data.rename(columns={"element": "fao_element", "unit": "fao_unit_short_name"}),
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
    assert len(data) == _expected_n_rows, "Something went wrong when merging data with elements metadata."

    # `category` type was lost during merge, convert it back
    data = data.astype(
        {
            "element_code": "category",
            "item_code": "category",
        }
    )

    # Remove "owid_" from column names.
    data = data.rename(columns={column: column.replace("owid_", "") for column in data.columns})

    return data


def remove_regions_from_countries_regions_members(
    countries_regions: pd.DataFrame, regions_to_remove: List[str]
) -> pd.DataFrame:
    """Remove regions that have to be ignored from the lists of members in the countries-regions dataset.

    Parameters
    ----------
    countries_regions : pd.DataFrame
        Countries-regions dataset (from the OWID catalog).
    regions_to_remove : list
        Regions to ignore.

    Returns
    -------
    countries_regions : pd.DataFrame
        Countries-regions dataset after removing regions from the lists of members of each country or region.

    """
    countries_regions = countries_regions.copy()

    # Get the owid code for each region that needs to be ignored when creating region aggregates.
    regions_to_ignore_codes = []
    for region in set(regions_to_remove):
        selected_region = countries_regions[countries_regions["name"] == region]
        assert len(selected_region) == 1, f"Region {region} ambiguous or not found in countries_regions dataset."
        regions_to_ignore_codes.append(selected_region.index.values.item())

    # Remove those regions to ignore from lists of members of each region.
    regions_mask = countries_regions["members"].notnull()
    countries_regions.loc[regions_mask, "members"] = [
        json.dumps(list(set(json.loads(members)) - set(regions_to_ignore_codes)))
        for members in countries_regions[regions_mask]["members"]
    ]

    return countries_regions


def load_population() -> pd.DataFrame:
    """Load OWID population dataset, and add historical regions to it.

    Returns
    -------
    population : pd.DataFrame
        Population dataset.

    """
    # Load population dataset.
    population = catalog.Dataset(DATA_DIR / "garden/owid/latest/key_indicators/")["population"].reset_index()[
        ["country", "year", "population"]
    ]

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
        population = pd.concat([population, _population], ignore_index=True).reset_index(drop=True)

    error = "Duplicate country-years found in population. Check if historical regions changed."
    assert population[population.duplicated(subset=["country", "year"])].empty, error

    return cast(pd.DataFrame, population)


def load_countries_regions() -> pd.DataFrame:
    """Load countries-regions dataset from the OWID catalog, and remove certain regions (defined in
    REGIONS_TO_IGNORE_IN_AGGREGATES) from the lists of members of countries or regions.

    Returns
    -------
    countries_regions : pd.DataFrame
        Countries-regions dataset.

    """
    # Load dataset of countries and regions.
    countries_regions = catalog.Dataset(DATA_DIR / "garden/regions/2023-01-01/regions")["regions"]

    countries_regions = remove_regions_from_countries_regions_members(
        countries_regions, regions_to_remove=REGIONS_TO_IGNORE_IN_AGGREGATES
    )

    return cast(pd.DataFrame, countries_regions)


def load_income_groups() -> pd.DataFrame:
    """Load dataset of income groups and add historical regions to it.

    Returns
    -------
    income_groups : pd.DataFrame
        Income groups data.

    """
    # Load the WorldBank dataset for income grups.
    income_groups = catalog.Dataset(DATA_DIR / "garden/wb/2021-07-01/wb_income")["wb_income_group"].reset_index()

    # Add historical regions to income groups.
    for historic_region in HISTORIC_TO_CURRENT_REGION:
        historic_region_income_group = HISTORIC_TO_CURRENT_REGION[historic_region]["income_group"]
        if historic_region not in income_groups["country"]:
            historic_region_df = pd.DataFrame(
                {
                    "country": [historic_region],
                    "income_group": [historic_region_income_group],
                }
            )
            income_groups = pd.concat([income_groups, historic_region_df], ignore_index=True)

    return cast(pd.DataFrame, income_groups)


def list_countries_in_region(region: str, countries_regions: pd.DataFrame, income_groups: pd.DataFrame) -> List[str]:
    """List all countries in a specific region or income group.

    Parameters
    ----------
    region : str
        Name of the region.
    countries_regions : pd.DataFrame
        Countries-regions dataset (after removing certain regions from the lists of members).
    income_groups : pd.DataFrame
        Dataset of income groups, which includes historical regions.

    Returns
    -------
    countries_in_regions : list
        List of countries in the given region or income group.

    """
    # Number of attempts to fetch countries regions data.
    attempts = 5
    attempt = 0
    countries_in_region = list()
    while attempt < attempts:
        try:
            # List countries in region.
            countries_in_region = geo.list_countries_in_region(
                region=region,
                countries_regions=countries_regions,
                income_groups=income_groups,
            )
            break
        except ConnectionResetError:
            attempt += 1
        finally:
            assert len(countries_in_region) > 0, "Unable to fetch countries-regions data."

    return countries_in_region


def remove_overlapping_data_between_historical_regions_and_successors(
    data_region: pd.DataFrame,
) -> pd.DataFrame:
    """Remove overlapping data between a historical region and any of its successors (if there is any overlap), to avoid
    double-counting those regions when aggregating data.

    Data for historical regions (e.g. USSR) could overlap with data of the successor countries (e.g. Russia). If this
    happens, remove data (on the overlapping element-item-years) of the historical country.

    Parameters
    ----------
    data_region : pd.DataFrame
        Data (after selecting the countries of a certain relevant region).

    Returns
    -------
    data_region : pd.DataFrame
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
        overlapping_years = pd.concat([historical_region_years, historical_successors_years], ignore_index=True)
        overlapping_years = overlapping_years[overlapping_years.duplicated()]
        if not overlapping_years.empty:
            log.warning(
                f"Removing rows where historical region {historical_region} overlaps with its successors "
                f"(years {sorted(set(overlapping_years['year']))})."
            )
            # Select rows in data_region to drop.
            overlapping_years["country"] = historical_region
            indexes_to_drop.extend(
                pd.merge(
                    data_region.reset_index(),
                    overlapping_years,
                    how="inner",
                    on=["country"] + columns,
                )["index"].tolist()
            )

    if len(indexes_to_drop) > 0:
        # Remove rows of data of the historical region where its data overlaps with data from its successors.
        data_region = data_region.drop(index=indexes_to_drop)

    return data_region


def remove_outliers(data: pd.DataFrame, outliers: List[Dict[str, List[Union[str, int]]]]) -> pd.DataFrame:
    """Remove known outliers (defined in OUTLIERS_TO_REMOVE) from processed data.

    The argument "outliers" is the list of outliers to remove: data points that are wrong and create artefacts in the
    charts. For each dictionary in the list, all possible combinations of the field values will be considered outliers
    (e.g. if two countries are given and three years, all three years will be removed for both countries).

    Parameters
    ----------
    data : pd.DataFrame
        Processed data (after harmonizing items, elements and countries, and adding regions and per-capita variables).
    outliers : list


    Returns
    -------
    data : pd.DataFrame
        Data after removing known outliers.

    """
    data = data.copy()

    # Make a dataframe with all rows that need to be removed from the data.
    rows_to_drop = pd.DataFrame()
    for outlier in outliers:
        # Find all possible combinations of the field values in the outlier dictionary (and ignore "notes").
        _outlier = {key: value for key, value in outlier.items() if key != "notes"}
        _rows_to_drop = pd.DataFrame.from_records(
            list(itertools.product(*_outlier.values())),
            columns=list(_outlier),
        )
        rows_to_drop = pd.concat([rows_to_drop, _rows_to_drop], ignore_index=True).reset_index(drop=True)

    # Quickly find out if there will be rows to drop in current dataset; if not, ignore.
    if (len(set(rows_to_drop["item_code"]) & set(data["item_code"])) > 0) & (
        len(set(rows_to_drop["element_code"]) & set(data["element_code"])) > 0
    ):
        log.info(f"Removing {len(rows_to_drop)} rows of known outliers.")

        # Get indexes of data that correspond to the rows we want to drop.
        indexes_to_drop = list(
            pd.merge(
                data.reset_index(),
                rows_to_drop,
                on=rows_to_drop.columns.tolist(),
                how="inner",
            )["index"].unique()
        )

        # Drop those rows in data.
        data = data.drop(indexes_to_drop).reset_index(drop=True)

    return data


def add_regions(data: pd.DataFrame, elements_metadata: pd.DataFrame) -> pd.DataFrame:
    """Add region aggregates (i.e. aggregate data for continents and income groups).

    Regions to be created are defined above, in REGIONS_TO_ADD, and the variables for which data will be aggregated are
    those that, in the custom_elements_and_units.csv file, have a non-empty 'owid_aggregation' field (usually with
    'sum', or 'mean'). The latter field determines the type of aggregation to create.

    Historical regions (if any) will be included in the aggregations, after ensuring that there is no overlap between
    the data for the region, and the data of any of its successor countries (for each item-element-year).

    Parameters
    ----------
    data : pd.DataFrame
        Clean data (after harmonizing items, element and countries).
    elements_metadata : pd.DataFrame
        Table 'elements' from the garden faostat_metadata dataset, after selecting elements for the current domain.

    Returns
    -------
    data : pd.DataFrame
        Data after adding rows for aggregate regions.

    """
    data = data.copy()

    # Create a dictionary of aggregations, specifying the operation to use when creating regions.
    # These aggregations are defined in the custom_elements_and_units.csv file, and added to the metadata dataset.
    aggregations = (
        elements_metadata[(elements_metadata["owid_aggregation"].notnull())]
        .set_index("element_code")
        .to_dict()["owid_aggregation"]
    )
    if len(aggregations) > 0:
        log.info("add_regions", shape=data.shape)

        # Load population dataset, countries-regions, and income groups datasets.
        population = load_population()
        countries_regions = load_countries_regions()
        income_groups = load_income_groups()

        # Invert dictionary of aggregations to have the aggregation as key, and the list of element codes as value.
        aggregations_inverted = {
            unique_value: pd.unique([item for item, value in aggregations.items() if value == unique_value]).tolist()
            for unique_value in aggregations.values()
        }
        for region in tqdm(REGIONS_TO_ADD, file=sys.stdout):
            countries_in_region = list_countries_in_region(
                region, countries_regions=countries_regions, income_groups=income_groups
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
                data_region = data[
                    (data["country"].isin(countries_in_region)) & (data["element_code"].isin(element_codes))
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
                    data_region = pd.merge(data_region, region_population, on="year", how="left")

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
                    data = dataframes.concatenate(
                        [data[data["country"] != region], data_region],
                        ignore_index=True,
                    )

            # Check that the fraction of population with data is as high as expected.
            frac_population = data["population_with_data"] / data["population"]
            assert frac_population[frac_population.notnull()].min() >= region_min_frac_population_with_data

        # Drop column of total population (we will still keep population_with_data).
        data = data.drop(columns=["population"])

        # Make area_code of category type (it contains integers and strings, and feather does not support object types).
        data["area_code"] = data["area_code"].astype(str).astype("category")

        # Sort conveniently.
        data = data.sort_values(["country", "year"]).reset_index(drop=True)

        check_that_countries_are_well_defined(data)

    return data


def add_fao_population_if_given(data: pd.DataFrame) -> pd.DataFrame:
    """Add a new column for FAO population, if population values are given in the data.

    Some datasets (e.g. faostat_fbsh and faostat_fbs) include per-capita variables from the beginning. When this
    happens, FAO population may be given as another item-element. To be able to convert those per-capita variables into
    total values, we need to extract that population data and make it a new column.

    Parameters
    ----------
    data : pd.DataFrame
        Data (after harmonizing elements and items, but before harmonizing countries).

    Returns
    -------
    data : pd.DataFrame
        Data, after adding a column 'fao_population', if FAO population was found in the data.

    """
    # Select rows that correspond to FAO population.
    fao_population_item_name = "Population"
    fao_population_element_name = "Total Population - Both sexes"
    population_rows_mask = (data["fao_item"] == fao_population_item_name) & (
        data["fao_element"] == fao_population_element_name
    )

    if population_rows_mask.any():
        data = data.copy()

        fao_population = data[population_rows_mask].reset_index(drop=True)

        # Check that population is given in "1000 persons" and convert to persons.
        assert list(fao_population["unit"].unique()) == ["1000 persons"], "FAO population may have changed units."
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
        data = pd.merge(data, fao_population, how="left", on=["area_code", "year"])

    return data


def add_population(
    df: pd.DataFrame,
    country_col: str = "country",
    year_col: str = "year",
    population_col: str = "population",
    warn_on_missing_countries: bool = True,
    show_full_warning: bool = True,
) -> pd.DataFrame:
    """Add a column of OWID population to the countries in the data, including population of historical regions.

    This function has been adapted from datautils.geo, because population currently does not include historic regions.
    We include them in this function.

    Parameters
    ----------
    df : pd.DataFrame
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
    df_with_population : pd.DataFrame
        Data after adding a column for population for all countries in the data.

    """

    # Load population dataset.
    population = load_population().rename(
        columns={
            "country": country_col,
            "year": year_col,
            "population": population_col,
        }
    )[[country_col, year_col, population_col]]

    # Check if there is any missing country.
    missing_countries = set(df[country_col]) - set(population[country_col])
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
    df_with_population = pd.merge(df, population, on=[country_col, year_col], how="left")

    return df_with_population


def convert_variables_given_per_capita_to_total_value(
    data: pd.DataFrame, elements_metadata: pd.DataFrame
) -> pd.DataFrame:
    """Replace variables given per capita in the original data by total values.

    NOTE:
    * Per-capita variables to be replaced by their total values are those with 'was_per_capita' equal to 1 in the
      custom_elements_and_units.csv file.
    * The new variables will have the same element codes as the original per-capita variables.

    Parameters
    ----------
    data : pd.DataFrame
        Data (after harmonizing elements and items, but before harmonizing countries).
    elements_metadata : pd.DataFrame
        Table 'elements' from the garden faostat_metadata dataset, after selecting the elements of the relevant domain.

    Returns
    -------
    data : pd.DataFrame
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
        data = data.copy()

        assert "fao_population" in data.columns, "fao_population not found, maybe it changed item, element."

        # Select variables that were given as per capita variables in the original data and that need to be converted.
        per_capita_mask = data["element_code"].isin(element_codes_that_were_per_capita)

        # Multiply them by the FAO population to convert them into total value.
        data.loc[per_capita_mask, "value"] = data[per_capita_mask]["value"] * data[per_capita_mask]["fao_population"]

        # Include an additional description to all elements that were converted from per capita to total variables.
        if "" not in data["element_description"].cat.categories:
            data["element_description"] = data["element_description"].cat.add_categories([""])
        data.loc[per_capita_mask, "element_description"] = data.loc[per_capita_mask, "element_description"].fillna("")
        data["element_description"] = dataframes.apply_on_categoricals(
            [data.element_description, per_capita_mask.astype("category")],
            lambda desc, mask: f"{desc} {WAS_PER_CAPITA_ADDED_ELEMENT_DESCRIPTION}".lstrip() if mask else f"{desc}",
        )

    return data


def add_per_capita_variables(data: pd.DataFrame, elements_metadata: pd.DataFrame) -> pd.DataFrame:
    """Add per-capita variables to data in a long format (and keep original variables as well).

    NOTE:
    * Variables for which new per-capita rows will be created are those with 'make_per_capita' equal to 1 in the
      custom_elements_and_units.csv file.
    * The new variables will have the same element codes as the original per-capita variables, with 'pc' prepended to
    the number.

    Parameters
    ----------
    data : pd.DataFrame
        Clean data (after harmonizing item codes and element codes, and countries, and adding aggregate regions).
    elements_metadata : pd.DataFrame
        Elements table from the garden faostat_metadata dataset, after selecting elements for the relevant domain.

    Returns
    -------
    data : pd.DataFrame
        Data with per-capita variables.

    """
    data = data.copy()

    # Find element codes that have to be made per capita.
    element_codes_to_make_per_capita = list(
        elements_metadata[elements_metadata["make_per_capita"]]["element_code"].unique()
    )
    if len(element_codes_to_make_per_capita) > 0:
        log.info("add_per_capita_variables", shape=data.shape)

        # Create a new dataframe that will have all per capita variables.
        per_capita_data = data[data["element_code"].isin(element_codes_to_make_per_capita)].reset_index(drop=True)

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
        data = dataframes.concatenate([data, per_capita_data], ignore_index=True).reset_index(drop=True)

    return data


def clean_data_values(values: pd.Series) -> pd.Series:
    """Fix spurious data values (defined in VALUE_AMENDMENTS) and make values a float column.

    Note: The mapping of VALUE_AMENDMENTS will only be applied if the input series of values is of type "category".
    At the moment, this fixes issues only in faostat_sdgb dataset.

    Parameters
    ----------
    values : pd.Series
        Content of the "value" column in the original data.

    Returns
    -------
    values_clean : pd.Series
        Original values after fixing known issues and converting to float.

    """
    values_clean = values.copy()
    if values_clean.dtype == "category":
        # Replace spurious values by either nan, or their correct numeric values (defined in VALUE_AMENDMENTS).
        values_clean = dataframes.map_series(
            series=values_clean,
            mapping=VALUE_AMENDMENTS,
            warn_on_missing_mappings=False,
            warn_on_unused_mappings=False,
        )

    # Convert all numbers into numeric.
    # Note: If this step fails with a ValueError, it may be because other spurious values have been introduced.
    # If so, add them to the VALUE_AMENDMENTS.
    values_clean = values_clean.astype(float)

    return values_clean


def clean_data(
    data: pd.DataFrame,
    items_metadata: pd.DataFrame,
    elements_metadata: pd.DataFrame,
    countries_metadata: pd.DataFrame,
) -> pd.DataFrame:
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
    data : pd.DataFrame
        Unprocessed data for current dataset (with harmonized item codes and element codes).
    items_metadata : pd.DataFrame
        Items metadata (from the metadata dataset) after selecting items for only the relevant domain.
    elements_metadata : pd.DataFrame
        Elements metadata (from the metadata dataset) after selecting elements for only the relevant domain.
    countries_metadata : pd.DataFrame
        Countries metadata (from the metadata dataset).

    Returns
    -------
    data : pd.DataFrame
        Processed data, ready to be made into a table for a garden dataset.

    """
    data = data.copy()

    # Fix spurious data values (applying mapping in VALUE_AMENDMENTS) and ensure column of values is float.
    data["value"] = clean_data_values(data["value"])

    # Convert nan flags into "official" (to avoid issues later on when dealing with flags).
    data["flag"] = pd.Series(
        [flag if not pd.isnull(flag) else FLAG_OFFICIAL_DATA for flag in data["flag"]],
        dtype="category",
    )

    # Some datasets (at least faostat_fa) use "recipient_country" instead of "area". For consistency, change this.
    data = data.rename(
        columns={
            "area": "fao_country",
            "recipient_country": "fao_country",
            "recipient_country_code": "area_code",
        }
    )

    # Ensure year column is integer (sometimes it is given as a range of years, e.g. 2013-2015).
    data["year"] = clean_year_column(data["year"])

    # Remove rows with nan value.
    data = remove_rows_with_nan_value(data)

    # Use custom names for items, elements and units (and keep original names in "fao_*" columns).
    data = add_custom_names_and_descriptions(data, items_metadata, elements_metadata)

    # Multiply data values by their corresponding unit factor, if any was given, and then drop unit_factor column.
    unit_factor_mask = data["unit_factor"].notnull()
    data.loc[unit_factor_mask, "value"] = data[unit_factor_mask]["value"] * data[unit_factor_mask]["unit_factor"]
    data = data.drop(columns=["unit_factor"])

    # Add FAO population as an additional column (if given in the original data).
    data = add_fao_population_if_given(data)

    # Convert variables that were given per-capita to total value.
    data = convert_variables_given_per_capita_to_total_value(data, elements_metadata=elements_metadata)

    # Harmonize country names.
    data = harmonize_countries(data=data, countries_metadata=countries_metadata)

    # Remove duplicated data points (if any) keeping the one with lowest ranking flag (i.e. highest priority).
    data = remove_duplicates(
        data=data,
        index_columns=["area_code", "year", "item_code", "element_code"],
        verbose=True,
    )

    # Add column for population; when creating region aggregates, this column will have the population of the countries
    # for which there was data. For example, for Europe in a specific year, the population may differ from item to item,
    # because for one item we may have more European countries informed than for the other.
    data = add_population(df=data, population_col="population_with_data", warn_on_missing_countries=False)

    # Convert back to categorical columns (maybe this should be handled automatically in `add_population_to_dataframe`)
    data = data.astype({"country": "category"})

    return data


def optimize_table_dtypes(table: catalog.Table) -> catalog.Table:
    """Optimize the dtypes of the columns in a table.

    NOTE: Using `.astype` in a loop over different columns is slow. Instead, it is better to map all columns at once or
    call `repack_frame` with dtypes arg

    Parameters
    ----------
    table : catalog.Table
        Table with possibly non-optimal column dtypes.

    Returns
    -------
    optimized_table : catalog.Table
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


def prepare_long_table(data: pd.DataFrame) -> catalog.Table:
    """Prepare a data table in long format.

    Parameters
    ----------
    data : pd.DataFrame
        Data (as a dataframe) in long format.

    Returns
    -------
    data_table_long : catalog.Table
        Data (as a table) in long format.

    """
    # Create new table with long data.
    data_table_long = catalog.Table(data)

    # Ensure table has the optimal dtypes before storing it as feather file.
    data_table_long = optimize_table_dtypes(table=data_table_long)

    # Set appropriate indexes.
    index_columns = ["area_code", "year", "item_code", "element_code"]
    data_table_long = data_table_long.set_index(index_columns, verify_integrity=True).sort_index()

    # Sanity check.
    number_of_infinities = len(data_table_long[data_table_long["value"] == np.inf])
    assert number_of_infinities == 0, f"There are {number_of_infinities} infinity values in the long table."

    return cast(catalog.Table, data_table_long)


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

    new_name = catalog.utils.underscore(variable_name)

    # Check that the number of characters of the short name is not too long.
    n_char = len(new_name)
    if n_char > 255:
        # This name will cause an issue when uploading to grapher (because of a limit of 255 characters in short name).
        # Remove the extra characters from the ending of the item name (if possible).
        n_char_to_be_removed = n_char - 255
        # It could happen that it is not the item name that is long, but the element name, dataset, or unit.
        # But for the moment, assume it is the item name.
        assert len(item) > n_char_to_be_removed, "Variable name is too long, but it is not due to item name."
        new_item = catalog.utils.underscore(item)[0:-n_char_to_be_removed]
        new_name = catalog.utils.underscore(f"{new_item} | {item_code} || {element} | {element_code} || {unit}")

    # Check that now the new name now fulfils the length requirement.
    error = "Variable short name is too long. Improve create_variable_names function to account for this case."
    assert len(new_name) <= 255, error

    return cast(str, new_name)


def prepare_wide_table(data: pd.DataFrame) -> catalog.Table:
    """Flatten a long table to obtain a wide table with ["country", "year"] as index.

    The input table will be pivoted to have [country, year] as index, and as many columns as combinations of
    item-element-unit entities.

    Parameters
    ----------
    data : pd.DataFrame
        Data for current domain.

    Returns
    -------
    wide_table : catalog.Table
        Data table with index [country, year].

    """
    data = data.copy(deep=False)

    # Ensure "item" exists in data (there are some datasets where it may be missing).
    if "item" not in data.columns:
        data["item"] = ""

    # Construct a variable name that will not yield any possible duplicates.
    # This will be used as column names (which will then be formatted properly with underscores and lower case),
    # and also as the variable titles in grapher.
    # Also, for convenience, keep a similar structure as in the previous OWID dataset release.
    # Finally, ensure that the short name version of the variable is not too long
    # (which would cause issues when uploading to grapher).
    data["variable_name"] = dataframes.apply_on_categoricals(
        [data.item, data.item_code, data.element, data.element_code, data.unit],
        lambda item, item_code, element, element_code, unit: f"{item} | {item_code} || {element} | {element_code} || {unit}",
    )

    # Construct a human-readable variable display name (which will be shown in grapher charts).
    data["variable_display_name"] = dataframes.apply_on_categoricals(
        [data.item, data.element, data.unit],
        lambda item, element, unit: f"{item} - {element} ({unit})",
    )

    # Construct a human-readable variable description (for the variable metadata).
    data["variable_description"] = dataframes.apply_on_categoricals(
        [data.item_description, data.element_description],
        lambda item_desc, element_desc: f"{item_desc}\n{element_desc}".lstrip().rstrip(),
    )

    # Pivot over long dataframe to generate a wide dataframe with country-year as index, and as many columns as
    # unique elements in "variable_name" (which should be as many as combinations of item-elements).
    # Note: We include area_code in the index for completeness, but by construction country-year should not have
    # duplicates.
    # Note: `pivot` operation is usually faster on categorical columns
    log.info("prepare_wide_table.pivot", shape=data.shape)
    # Create a wide table with just the data values.
    wide_table = catalog.Table(
        data.pivot(
            index=["area_code", "country", "year"],
            columns=["variable_name"],
            values="value",
        )
    )

    # Add metadata to each new variable in the wide data table.
    log.info("prepare_wide_table.adding_metadata", shape=wide_table.shape)

    # Add variable name.
    for column in wide_table.columns:
        wide_table[column].metadata.title = column

    # Add variable unit (long name).
    variable_name_mapping = _variable_name_map(data, "unit")
    for column in wide_table.columns:
        wide_table[column].metadata.unit = variable_name_mapping[column]

    # Add variable unit (short name).
    variable_name_mapping = _variable_name_map(data, "unit_short_name")
    for column in wide_table.columns:
        wide_table[column].metadata.short_unit = variable_name_mapping[column]

    # Add variable description.
    variable_name_mapping = _variable_name_map(data, "variable_description")
    for column in wide_table.columns:
        wide_table[column].metadata.description = variable_name_mapping[column]

    # Add display parameters (for grapher).
    for column in wide_table.columns:
        wide_table[column].metadata.display = {}

    # Display name.
    variable_name_mapping = _variable_name_map(data, "variable_display_name")
    for column in wide_table.columns:
        wide_table[column].metadata.display["name"] = variable_name_mapping[column]

    # Ensure columns have the optimal dtypes, but codes are categories.
    log.info("prepare_wide_table.optimize_table_dtypes", shape=wide_table.shape)
    wide_table = optimize_table_dtypes(table=wide_table.reset_index())

    # Sort columns and rows conveniently.
    wide_table = wide_table.set_index(["country", "year"], verify_integrity=True)
    wide_table = wide_table[["area_code"] + sorted([column for column in wide_table.columns if column != "area_code"])]
    wide_table = wide_table.sort_index(level=["country", "year"]).sort_index()

    # Make all column names snake_case.
    variable_to_short_name = {
        column: create_variable_short_names(variable_name=wide_table[column].metadata.title)
        for column in wide_table.columns
        if wide_table[column].metadata.title is not None
    }
    wide_table = wide_table.rename(columns=variable_to_short_name, errors="raise")

    # Sanity check.
    number_of_infinities = np.isinf(wide_table.select_dtypes(include=np.number).fillna(0)).values.sum()
    assert number_of_infinities == 0, f"There are {number_of_infinities} infinity values in the wide table."

    return wide_table


def _variable_name_map(data: pd.DataFrame, column: str) -> Dict[str, str]:
    """Extract map {variable name -> column} from dataframe and make sure it is unique (i.e. ensure that one variable
    does not map to two distinct values)."""
    pivot = data.dropna(subset=[column]).groupby(["variable_name"], observed=True)[column].apply(set)
    assert all(pivot.map(len) == 1)
    return pivot.map(lambda x: list(x)[0]).to_dict()  # type: ignore


def run(dest_dir: str) -> None:
    ####################################################################################################################
    # Common definitions.
    ####################################################################################################################

    # Assume dest_dir is a path to the step to be run, e.g. "faostat_qcl", and get the dataset short name from it.
    dataset_short_name = Path(dest_dir).name
    # Path to dataset of FAOSTAT metadata.
    garden_metadata_dir = DATA_DIR / "garden" / NAMESPACE / VERSION / f"{NAMESPACE}_metadata"

    # Path to outliers file.
    outliers_file = STEP_DIR / "data" / "garden" / NAMESPACE / VERSION / "detected_outliers.json"

    ####################################################################################################################
    # Load data.
    ####################################################################################################################

    # Load file of versions.
    latest_versions = pd.read_csv(LATEST_VERSIONS_FILE).set_index(["channel", "dataset"])

    # Path to latest dataset in meadow for current FAOSTAT domain.
    meadow_version = latest_versions.loc["meadow", dataset_short_name].item()
    meadow_data_dir = DATA_DIR / "meadow" / NAMESPACE / meadow_version / dataset_short_name
    # Load latest meadow dataset and keep its metadata.
    dataset_meadow = catalog.Dataset(meadow_data_dir)
    # Load main table from dataset.
    data_table_meadow = dataset_meadow[dataset_short_name]
    data = pd.DataFrame(data_table_meadow).reset_index()

    # Load dataset of FAOSTAT metadata.
    metadata = catalog.Dataset(garden_metadata_dir)

    # Load and prepare dataset, items, element-units, and countries metadata.
    datasets_metadata = pd.DataFrame(metadata["datasets"]).reset_index()
    datasets_metadata = datasets_metadata[datasets_metadata["dataset"] == dataset_short_name].reset_index(drop=True)
    items_metadata = pd.DataFrame(metadata["items"]).reset_index()
    items_metadata = items_metadata[items_metadata["dataset"] == dataset_short_name].reset_index(drop=True)
    elements_metadata = pd.DataFrame(metadata["elements"]).reset_index()
    elements_metadata = elements_metadata[elements_metadata["dataset"] == dataset_short_name].reset_index(drop=True)
    countries_metadata = pd.DataFrame(metadata["countries"]).reset_index()

    # Load file of detected outliers.
    with open(outliers_file, "r") as _json_file:
        outliers = json.loads(_json_file.read())

    ####################################################################################################################
    # Process data.
    ####################################################################################################################

    # Harmonize items and elements, and clean data.
    data = harmonize_items(df=data, dataset_short_name=dataset_short_name)
    data = harmonize_elements(df=data)

    # Prepare data.
    data = clean_data(
        data=data,
        items_metadata=items_metadata,
        elements_metadata=elements_metadata,
        countries_metadata=countries_metadata,
    )

    # Add data for aggregate regions.
    data = add_regions(data=data, elements_metadata=elements_metadata)

    # Add per-capita variables.
    data = add_per_capita_variables(data=data, elements_metadata=elements_metadata)

    # Remove outliers (this step needs to happen after creating regions and per capita variables).
    data = remove_outliers(data, outliers=outliers)

    # Create a long table (with item code and element code as part of the index).
    data_table_long = prepare_long_table(data=data)

    # Create a wide table (with only country and year as index).
    data_table_wide = prepare_wide_table(data=data)

    ####################################################################################################################
    # Save outputs.
    ####################################################################################################################

    # Initialize new garden dataset.
    dataset_garden = catalog.Dataset.create_empty(dest_dir)
    # Prepare metadata for new garden dataset (starting with the metadata from the meadow version).
    dataset_garden_metadata = deepcopy(dataset_meadow.metadata)
    dataset_garden_metadata.version = VERSION
    dataset_garden_metadata.description = datasets_metadata["owid_dataset_description"].item()
    dataset_garden_metadata.title = datasets_metadata["owid_dataset_title"].item()
    # Add metadata to dataset.
    dataset_garden.metadata = dataset_garden_metadata
    # Create new dataset in garden.
    dataset_garden.save()

    # Prepare metadata for new garden long table (starting with the metadata from the meadow version).
    data_table_long.metadata = deepcopy(data_table_meadow.metadata)
    data_table_long.metadata.title = dataset_garden_metadata.title
    data_table_long.metadata.description = dataset_garden_metadata.description
    data_table_long.metadata.primary_key = list(data_table_long.index.names)
    data_table_long.metadata.dataset = dataset_garden_metadata
    # Add long table to the dataset (no need to repack, since columns already have optimal dtypes).
    dataset_garden.add(data_table_long, repack=False)

    # Prepare metadata for new garden wide table (starting with the metadata from the long table).
    # Add wide table to the dataset.
    data_table_wide.metadata = deepcopy(data_table_long.metadata)

    data_table_wide.metadata.title += ADDED_TITLE_TO_WIDE_TABLE
    data_table_wide.metadata.short_name += "_flat"
    data_table_wide.metadata.primary_key = list(data_table_wide.index.names)

    # Add wide table to the dataset (no need to repack, since columns already have optimal dtypes).
    dataset_garden.add(data_table_wide, repack=False)
