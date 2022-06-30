"""Common processing of FAOSTAT datasets.

We have created a manual ranking of FAOSTAT flags. These flags are only used when there is ambiguity in the data,
namely, when there is more than one data value for a certain country-year-item-element-unit.
NOTES:
* We check that the definitions in our manual ranking agree with the ones provided by FAOSTAT.
* We do not include all flags: We include only the ones that solve an ambiguity in a particular case,
  and add more flags as we see need.
* We have found flags that appeared in a dataset, but were not included in the additional metadata
  (namely flag "R", found in qcl dataset, and "W" in rt dataset). These flags were added manually, using the definition
  in List / Flags in:
  https://www.fao.org/faostat/en/#definitions
* Other flags (namely "B", in rl dataset and "w" in rt dataset) were not found either in the additional metadata or in
  the website definitions. They have been assigned the description "Unknown flag".
* Unfortunately, flags do not remove all ambiguities: remaining duplicates are dropped without any meaningful criterion.

"""

import itertools
import json
import sys
from copy import deepcopy
from pathlib import Path
from typing import List, cast, Dict

import structlog
import numpy as np
import pandas as pd
from owid import catalog
from owid.datautils import dataframes, geo
from tqdm.auto import tqdm

from etl.paths import DATA_DIR

log = structlog.get_logger()

NAMESPACE = Path(__file__).parent.parent.name
VERSION = Path(__file__).parent.name

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
        }
    ],
}

# When creating region aggregates for a certain variable in a certain year, we want to ensure that we have enough
# data to create the aggregate. There is no straightforward way to do so. Our criterion is to:
#  * sum the data of all countries in the region, and then
#  * remove rows such that the sum of the population of countries with data (for a given year) is too small, compared
#    to the total population of the region.
# For example, if for a certain variable in a certain year, only a few countries with little population have data,
# then assign nan to that region-variable-year.
# Define here that minimum fraction of population that must have data to create an aggregate.
MIN_FRAC_POPULATION_WITH_DATA = 0.
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

# Additional explanation to append to element description for variables that were originally given per capita.
WAS_PER_CAPITA_ADDED_ELEMENT_DESCRIPTION = "Originally given per-capita, and converted into total figures by " \
                                           "multiplying by population (given by FAO)."
# Additional explanation to append to element description for created per-capita variables.
NEW_PER_CAPITA_ADDED_ELEMENT_DESCRIPTION = "Per-capita values are obtained by dividing the original values by the " \
                                           "population (either provided by FAO or by OWID)."

# When creating region aggregates, we need to ignore geographical regions that contain aggregate data from other
# countries, to avoid double-counting the data of those countries.
# Note: This list does not contain all country groups, but only those that are in our list of harmonized countries
# (without the *(FAO) suffix).
REGIONS_TO_IGNORE_IN_AGGREGATES = [
    'Melanesia',
    'Polynesia',
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

# Additional text to include in the metadata title of the output wide table.
ADDED_TITLE_TO_WIDE_TABLE = " - Flattened table indexed by country-year."

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
            ("W", "Data reported on country official publications or web sites (Official) or trade country files"),
            ("Q", "Official data reported on FAO Questionnaires from countries"),
            ("Qm", "Official data from questionnaires and/or national sources and/or COMTRADE (reporters)"),
            ("E", "Expert sources from FAO (including other divisions)"),
            ("I", "Country data reported by International Organizations where the country is a member (Semi-official) "
                  "- WTO, EU, UNSD, etc."),
            ("A", "Aggregate, may include official, semi-official, estimated or calculated data"),
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
            ("Z", "When the Fertilizer Utilization Account (FUA) does not balance due to utilization from stockpiles, "
                  "apparent consumption has been set to zero"),
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

# Outliers to remove (data points that are wrong and create artefacts in the charts).
# For each dictionary, all possible combinations of the field values will be considered
# (e.g. if two countries are given and three years, all three years will be removed for both countries).
OUTLIERS_TO_REMOVE = [
    # China mainland in 1984 has no data for area harvested of Spinach. This causes a big dip in China
    # (the aggregate of mainland, Hong Kong, Taiwan and Macau), as well as other regions containing China.
    {
        # Harmonized country names affected by outliers.
        "country": ["China (FAO)", "Asia", "Asia (FAO)", "Upper-middle-income countries", "World",
                    "Eastern Asia (FAO)"],
        "year": [1984],
        # Item code for "Spinach".
        "item_code": ["00000373"],
        # Element codes for "Area harvested" (total and per-capita) and "Yield".
        "element_code": ["005312", "5312pc", "005419"],
    }
]


def check_that_countries_are_well_defined(data):
    # Ensure area codes and countries are well defined, and no ambiguities were introduced when mapping country names.
    n_countries_per_area_code = data.groupby("area_code")["country"].transform("nunique")
    ambiguous_area_codes = data[n_countries_per_area_code > 1][["area_code", "country"]].\
        drop_duplicates().set_index("area_code")["country"].to_dict()
    error = f"There cannot be multiple countries for the same area code. " \
            f"Redefine countries file for:\n{ambiguous_area_codes}."
    assert len(ambiguous_area_codes) == 0, error
    n_area_codes_per_country = data.groupby("country")["area_code"].transform("nunique")
    ambiguous_countries = data[n_area_codes_per_country > 1][["area_code", "country"]].\
        drop_duplicates().set_index("area_code")["country"].to_dict()
    error = f"There cannot be multiple area codes for the same countries. " \
            f"Redefine countries file for:\n{ambiguous_countries}."
    assert len(ambiguous_countries) == 0, error


def check_that_regions_with_subregions_are_ignored_when_constructing_aggregates(countries_metadata):
    # Check if there is any harmonized regions that contain subregions.
    # If so, they should be ignored when constructing region aggregates, to avoid double-counting them.
    countries_with_subregions = countries_metadata[
        (countries_metadata["country"] != "World") &
        (~countries_metadata["country"].isin(REGIONS_TO_ADD)) &
        (~countries_metadata["country"].isin(REGIONS_TO_IGNORE_IN_AGGREGATES)) &
        (~countries_metadata["country"].str.contains("(FAO)", regex=False).fillna(False)) &
        (countries_metadata["members"].notnull())]["country"].unique().tolist()

    error = f"Regions {countries_with_subregions} contain subregions. Add them to REGIONS_TO_IGNORE_IN_AGGREGATES to " \
            f"avoid double-counting subregions when constructing aggregates."
    assert len(countries_with_subregions) == 0, error


def harmonize_items(df, dataset_short_name, item_col="item") -> pd.DataFrame:
    df = df.copy()
    # Note: Here list comprehension is faster than doing .astype(str).str.zfill(...).
    df["item_code"] = [str(item_code).zfill(N_CHARACTERS_ITEM_CODE) for item_code in df["item_code"]]
    df[item_col] = df[item_col].astype(str)

    # Fix those few cases where there is more than one item per item code within a given dataset.
    if dataset_short_name in ITEM_AMENDMENTS:
        for amendment in ITEM_AMENDMENTS[dataset_short_name]:
            df.loc[(df["item_code"] == amendment["item_code"]) &
                   (df[item_col] == amendment["fao_item"]), ("item_code", item_col)] = \
                (amendment["new_item_code"], amendment["new_fao_item"])

    # Convert both columns to category to reduce memory
    df = df.astype({
        'item_code': 'category',
        item_col: 'category'
    })

    return df


def harmonize_elements(df, element_col="element") -> pd.DataFrame:
    df = df.copy()
    df["element_code"] = [str(element_code).zfill(N_CHARACTERS_ELEMENT_CODE) for element_code in df["element_code"]]

    # Convert both columns to category to reduce memory
    df = df.astype({
        'element_code': 'category',
        element_col: 'category'
    })

    return df


def harmonize_countries(data, countries_metadata):
    data = data.copy()

    if data["area_code"].dtype == "float64":
        # This happens at least for faostat_sdgb, where area code is totally different to the usual one.
        # See further explanations in garden step for faostat_metadata.
        # When this happens, merge using the old country name instead of the area code.
        data = pd.merge(data, countries_metadata[["fao_country", "country"]], on="fao_country", how="left")
    else:
        # Add harmonized country names (from countries metadata) to data.
        data = pd.merge(data, countries_metadata[["area_code", "fao_country", "country"]].
                        rename(columns={"fao_country": "fao_country_check"}), on="area_code", how="left")
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


def remove_rows_with_nan_value(
    data: pd.DataFrame, verbose: bool = False
) -> pd.DataFrame:
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
            log.info(
                f"Removing {n_rows_with_nan_value} rows ({frac_nan_rows: .2%}) "
                f"with nan in column 'value'."
            )
        if frac_nan_rows > 0.15:
            log.warning(f"{frac_nan_rows: .0%} rows of nan values removed.")
        data = data.dropna(subset="value").reset_index(drop=True)

    return data


def remove_columns_with_only_nans(
    data: pd.DataFrame, verbose: bool = True
) -> pd.DataFrame:
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
        data = data.sort_values(_index_columns + ["flag_ranking"]).drop_duplicates(
            subset=_index_columns, keep="first"
        )
        frac_ambiguous = n_ambiguous_indexes / len(data)
        frac_ambiguous_solved_by_flags = 1 - (
            n_ambiguous_indexes_unsolvable / n_ambiguous_indexes
        )
        if verbose:
            log.info(f"Removing {n_ambiguous_indexes} ambiguous indexes ({frac_ambiguous: .2%}). "
                     f"{frac_ambiguous_solved_by_flags: .2%} of ambiguities were solved with flags.")

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


def add_custom_names_and_descriptions(data, items_metadata, elements_metadata):
    data = data.copy()

    error = f"There are missing item codes in metadata."
    assert set(data["item_code"]) <= set(items_metadata["item_code"]), error

    error = f"There are missing element codes in metadata."
    assert set(data["element_code"]) <= set(elements_metadata["element_code"]), error

    _expected_n_rows = len(data)
    data = pd.merge(data.rename(columns={"item": "fao_item"}),
                    items_metadata[['item_code', 'owid_item', 'owid_item_description']], on="item_code", how="left")
    assert len(data) == _expected_n_rows, f"Something went wrong when merging data with items metadata."

    data = pd.merge(data.rename(columns={"element": "fao_element", "unit": "fao_unit"}),
                    elements_metadata[['element_code', 'owid_element', 'owid_unit', 'owid_unit_factor',
                                       'owid_element_description', 'owid_unit_short_name']],
                    on=["element_code"], how="left")
    assert len(data) == _expected_n_rows, f"Something went wrong when merging data with elements metadata."

    # `category` type was lost during merge, convert it back
    data = data.astype({
        "element_code": "category",
        "item_code": "category",
    })

    # Remove "owid_" from column names.
    data = data.rename(columns={column: column.replace("owid_", "") for column in data.columns})

    return data


def remove_regions_from_countries_regions_members(countries_regions, regions_to_remove):
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
        for members in countries_regions[regions_mask]["members"]]

    return countries_regions


def _load_population() -> pd.DataFrame:
    # Load population dataset.
    population = catalog.find("population", namespace="owid", dataset="key_indicators").load().\
        reset_index()[["country", "year", "population"]]

    # Add data for historical regions (if not in population) by adding the population of its current successors.
    countries_with_population = population["country"].unique()
    missing_countries = [country for country in HISTORIC_TO_CURRENT_REGION
                         if country not in countries_with_population]
    for country in missing_countries:
        members = HISTORIC_TO_CURRENT_REGION[country]["members"]
        _population = population[population["country"].isin(members)].\
            groupby("year").agg({"population": "sum", "country": "nunique"}).reset_index()
        # Select only years for which we have data for all member countries.
        _population = _population[_population["country"] == len(members)].reset_index(drop=True)
        _population["country"] = country
        population = pd.concat([population, _population], ignore_index=True).reset_index(drop=True)

    error = "Duplicate country-years found in population. Check if historical regions changed."
    assert population[population.duplicated(subset=["country", "year"])].empty, error

    return cast(pd.DataFrame, population)


def _load_countries_regions() -> pd.DataFrame:
    # Load dataset of countries and regions.
    countries_regions = catalog.find("countries_regions", dataset="reference", namespace="owid").load()

    countries_regions = remove_regions_from_countries_regions_members(
        countries_regions, regions_to_remove=REGIONS_TO_IGNORE_IN_AGGREGATES)

    return cast(pd.DataFrame, countries_regions)


def _load_income_groups() -> pd.DataFrame:
    income_groups = catalog.find(table="wb_income_group", dataset="wb_income", namespace="wb", channels=["garden"]).\
        load().reset_index()
    # Add historical regions to income groups.
    for historic_region in HISTORIC_TO_CURRENT_REGION:
        historic_region_income_group = HISTORIC_TO_CURRENT_REGION[historic_region]["income_group"]
        if historic_region not in income_groups["country"]:
            historic_region_df = pd.DataFrame({"country": [historic_region],
                                               "income_group": [historic_region_income_group]})
            income_groups = pd.concat([income_groups, historic_region_df], ignore_index=True)

    return income_groups


def _list_countries_in_region(region, countries_regions, income_groups):
    # Number of attempts to fetch countries regions data.
    attempts = 5
    attempt = 0
    countries_in_region = None
    while attempt < attempts:
        try:
            # List countries in region.
            countries_in_region = geo.list_countries_in_region(region=region, countries_regions=countries_regions,
                                                               income_groups=income_groups)
            break
        except ConnectionResetError:
            attempt += 1
        finally:
            assert countries_in_region is not None, "Unable to fetch countries-regions data."

    return countries_in_region


def remove_overlapping_data_between_historical_regions_and_successors(data_region):
    data_region = data_region.copy()
    # Sometimes, data for historical regions (e.g. USSR) overlaps with data of the successor countries (e.g. Russia).
    # Ideally, we would keep only data for the newer countries. However, if not all successors have data, we would be
    # having an incomplete aggregation, and therefore it would be better to keep data from the historical region.
    columns = ["item_code", "element_code", "year"]

    indexes_to_drop = []
    for historical_region in HISTORIC_TO_CURRENT_REGION:
        historical_successors = HISTORIC_TO_CURRENT_REGION[historical_region]["members"]
        historical_region_years = data_region[(data_region["country"] == historical_region)][columns].\
            drop_duplicates()
        historical_successors_years = data_region[(data_region["country"].isin(historical_successors))][columns].\
            drop_duplicates()
        overlapping_years = pd.concat([historical_region_years, historical_successors_years], ignore_index=True)
        overlapping_years = overlapping_years[overlapping_years.duplicated()]
        if not overlapping_years.empty:
            log.warning(f"Removing rows where historical region {historical_region} overlaps with its successors "
                        f"(years {sorted(set(overlapping_years['year']))}).")
            # Select rows in data_region to drop.
            overlapping_years["country"] = historical_region
            indexes_to_drop.extend(pd.merge(data_region.reset_index(), overlapping_years, how='inner',
                                            on=["country"] + columns)["index"].tolist())

    if len(indexes_to_drop) > 0:
        data_region = data_region.drop(index=indexes_to_drop)

    return data_region


def remove_outliers(data):
    data = data.copy()

    # Make a dataframe with all rows that need to be removed from the data.
    rows_to_drop = pd.DataFrame()
    for outlier in OUTLIERS_TO_REMOVE:
        # Find all possible combinations of the field values in the outlier dictionary.
        _rows_to_drop = pd.DataFrame.from_records(list(itertools.product(*outlier.values())), columns=list(outlier))
        rows_to_drop = pd.concat([rows_to_drop, _rows_to_drop], ignore_index=True).reset_index(drop=True)

    # Quickly find out if there will be rows to drop in current dataset; if not, ignore.
    if (len(set(rows_to_drop["item_code"]) & set(data["item_code"])) > 0) & \
        (len(set(rows_to_drop["element_code"]) & set(data["element_code"])) > 0):
        log.info(f"Removing {len(rows_to_drop)} rows of outliers.")

        # Get indexes of data that correspond to the rows we want to drop.
        indexes_to_drop = pd.merge(data.reset_index(), rows_to_drop, on=rows_to_drop.columns.tolist(),
                                   how="inner")["index"].unique().tolist()

        # Drop those rows in data.
        data = data.drop(indexes_to_drop).reset_index(drop=True)

    return data


def add_regions(data, elements_metadata):
    data = data.copy()

    # Create a dictionary of aggregations, specifying the operation to use when creating regions.
    # These aggregations are defined in the custom_elements_and_units.csv file, and added to the metadata dataset.
    aggregations = elements_metadata[(elements_metadata["owid_aggregation"].notnull())].\
        set_index("element_code").to_dict()["owid_aggregation"]
    if len(aggregations) > 0:
        log.info("add_regions", shape=data.shape)

        # Load population dataset, countries-regions, and income groups datasets.
        population = _load_population()
        countries_regions = _load_countries_regions()
        income_groups = _load_income_groups()

        # Invert dictionary of aggregations to have the aggregation as key, and the list of element codes as value.
        aggregations_inverted = {unique_value: pd.unique([item for item, value in aggregations.items()
                                                          if value == unique_value]).tolist()
                                 for unique_value in aggregations.values()}
        for region in tqdm(REGIONS_TO_ADD, file=sys.stdout):
            countries_in_region = _list_countries_in_region(
                region, countries_regions=countries_regions, income_groups=income_groups)
            region_code = REGIONS_TO_ADD[region]["area_code"]
            region_population = population[population["country"] == region][["year", "population"]].\
                reset_index(drop=True)
            region_min_frac_population_with_data = REGIONS_TO_ADD[region]["min_frac_population_with_data"]
            for aggregation in aggregations_inverted:
                # List of element codes for which the same aggregate method (e.g. "sum") will be applied.
                element_codes = aggregations_inverted[aggregation]

                # Select relevant rows in the data.
                data_region = data[(data["country"].isin(countries_in_region)) &
                                   (data["element_code"].isin(element_codes))]

                # Ensure there is no overlap between historical regions and their successors.
                data_region = remove_overlapping_data_between_historical_regions_and_successors(data_region)

                if len(data_region) > 0:
                    data_region = dataframes.groupby_agg(
                        df=data_region.dropna(subset="value"), groupby_columns=[
                            "year", "item_code", "element_code",
                            "item",
                            "element",
                            "fao_element",
                            "fao_item",
                            "item_description",
                            "unit",
                            "unit_short_name",
                            "fao_unit",
                            "element_description"],
                        num_allowed_nans=None, frac_allowed_nans=None,
                        aggregations={
                            "value": aggregation,
                            "flag": lambda x: x if len(x) == 1 else FLAG_MULTIPLE_FLAGS,
                            "population_with_data": "sum",
                        }
                    ).reset_index().dropna(subset="element")

                    # Add total population of the region (for each year) to the relevant data.
                    data_region = pd.merge(data_region, region_population, on="year", how="left")

                    # Keep only rows for which we have sufficient data.
                    data_region = data_region[(data_region["population_with_data"] / data_region["population"])
                                              >= region_min_frac_population_with_data].reset_index(drop=True)

                    # Add region's name and area code.
                    data_region["country"] = region
                    data_region["area_code"] = region_code

                    # Use category type which is more efficient than using strings
                    data_region = data_region.astype({
                        "flag": "category",
                        "country": "category",
                    })

                    # Add data for current region to data.
                    data = dataframes.concatenate([data[data["country"] != region], data_region], ignore_index=True)

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


def add_fao_population_if_given(data):
    # Select rows that correspond to FAO population.
    fao_population_item_name = "Population"
    fao_population_element_name = "Total Population - Both sexes"
    population_rows_mask = (data["fao_item"] == fao_population_item_name) &\
                           (data["fao_element"] == fao_population_element_name)

    if population_rows_mask.any():
        data = data.copy()

        fao_population = data[population_rows_mask].reset_index(drop=True)

        # Check that population is given in "1000 persons" and convert to persons.
        assert fao_population["unit"].unique().tolist() == ["1000 persons"], "FAO population may have changed units."
        fao_population["value"] *= 1000

        # Note: Here we will dismiss the flags related to population. But they are only relevant for those columns
        # that were given as per capita variables.
        fao_population = fao_population[["area_code", "year", "value"]].drop_duplicates().\
            dropna(how="any").rename(columns={"value": "fao_population"})

        # Add FAO population as a new column in data.
        data = pd.merge(data, fao_population, how="left", on=["area_code", "year"])

    return data


def add_population(df: pd.DataFrame, country_col: str = "country", year_col: str = "year",
                   population_col: str = "population", warn_on_missing_countries: bool = True,
                   show_full_warning: bool = True):
    # This function has been adapted from datautils.geo, because population currently does not include historic
    # regions. We include them here.

    # Load population dataset.
    population = _load_population().rename(
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
    df_with_population = pd.merge(
        df, population, on=[country_col, year_col], how="left"
    )

    return df_with_population


def convert_variables_given_per_capita_to_total_value(data, elements_metadata):
    # Select element codes that were originally given as per capita variables (if any), and, if FAO population is
    # given, make them total variables instead of per capita.
    # All variables in the custom_elements_and_units.csv file with "was_per_capita" True will be converted into
    # total (non-per-capita) values.
    element_codes_that_were_per_capita = elements_metadata[elements_metadata["was_per_capita"]]["element_code"].\
        unique().tolist()
    if len(element_codes_that_were_per_capita) > 0:
        data = data.copy()

        assert "fao_population" in data.columns, "fao_population not found, maybe it changed item, element."

        # Select variables that were given as per capita variables in the original data and that need to be converted.
        per_capita_mask = data["element_code"].isin(element_codes_that_were_per_capita)

        # Multiply them by the FAO population to convert them into total value.
        data.loc[per_capita_mask, "value"] = data[per_capita_mask]["value"] * data[per_capita_mask]["fao_population"]

        elements_converted = data[per_capita_mask]["fao_element"].unique().tolist()
        log.info(f"{len(elements_converted)} elements converted from per-capita to total values: {elements_converted}")

        # Include an additional description to all elements that were converted from per capita to total variables.
        if "" not in data["element_description"].cat.categories:
            data["element_description"] = data["element_description"].cat.add_categories([""])
        data.loc[per_capita_mask, "element_description"] = data.loc[per_capita_mask, "element_description"].fillna('')
        data["element_description"] = dataframes.apply_on_categoricals(
            [data.element_description, per_capita_mask.astype("category")],
            lambda desc, mask: f"{desc} {WAS_PER_CAPITA_ADDED_ELEMENT_DESCRIPTION}".lstrip() if mask else f"{desc}",
        )

    return data


def add_per_capita_variables(data, elements_metadata):
    data = data.copy()

    # Find element codes that have to be made per capita.
    element_codes_to_make_per_capita = elements_metadata[elements_metadata["make_per_capita"]]["element_code"].\
        unique().tolist()
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
        fao_regions_mask = (per_capita_data["country"].str.contains("(FAO)", regex=False))
        # Create a mask that selects all other regions (i.e. harmonized countries).
        owid_regions_mask = ~fao_regions_mask

        # Create per capita variables for FAO regions (this can only be done if a column for FAO population is given).
        if "fao_population" in per_capita_data.columns:
            per_capita_data.loc[fao_regions_mask, "value"] = per_capita_data[fao_regions_mask]["value"] /\
                per_capita_data[fao_regions_mask]["fao_population"]
        else:
            # Per capita variables can't be created for FAO regions, since we don't have FAO population.
            # Remove these regions from the per capita dataframe; only OWID harmonized countries will be kept.
            per_capita_data = per_capita_data[~fao_regions_mask].reset_index(drop=True)
            owid_regions_mask = np.ones(len(per_capita_data), dtype=bool)

        # Add per capita values to all other regions that are not FAO regions.
        per_capita_data.loc[owid_regions_mask, "value"] = per_capita_data[owid_regions_mask]["value"] /\
            per_capita_data[owid_regions_mask]["population_with_data"]

        # Remove nans (which may have been created because of missing FAO population).
        per_capita_data = per_capita_data.dropna(subset="value").reset_index(drop=True)

        # Add "per capita" to all units.
        per_capita_data["unit"] = per_capita_data["unit"].cat.rename_categories(
            lambda c: f"{c} per capita"
        )
        # Include an additional note in the description on affected elements.
        per_capita_data["element_description"] = per_capita_data["element_description"].cat.rename_categories(
            lambda c: f"{c} {NEW_PER_CAPITA_ADDED_ELEMENT_DESCRIPTION}"
        )
        # Add new rows with per capita variables to data.
        data = dataframes.concatenate([data, per_capita_data], ignore_index=True).reset_index(drop=True)

    return data


def clean_data(data: pd.DataFrame, items_metadata: pd.DataFrame, elements_metadata: pd.DataFrame,
               countries_metadata: pd.DataFrame) -> pd.DataFrame:
    """Process data (including harmonization of countries and regions) and prepare it for new garden dataset.

    Parameters
    ----------
    data : pd.DataFrame
        Unprocessed data for current dataset.
    items_metadata : pd.DataFrame
        Items metadata (from the metadata dataset).
    elements_metadata : pd.DataFrame
        Elements metadata (from the metadata dataset).
    countries_metadata : pd.DataFrame
        Countries metadata (from the metadata dataset).

    Returns
    -------
    data : pd.DataFrame
        Processed data, ready to be made into a table for a garden dataset.

    """
    data = data.copy()

    # Ensure column of values is numeric (transform any possible value like "<1" into a nan).
    # TODO: Dataset faostat_sdgb contains instances of numbers like "<1" or "<2.5" or comma for thousands "1,173.92",
    #  which they will be converted to nan here. Instead, create a function that properly cleans the values.
    data["value"] = pd.to_numeric(data["value"], errors="coerce")

    # Ensure column of values is float.
    # Note: Int64 would also work, but when dividing by a float, it changes to Float64 dtype, which, for some reason,
    # makes nans undetectable (i.e. .isnull() does not detect nans and .dropna() does not drop nans).
    data["value"] = data["value"].astype(float)

    # Convert nan flags into "official" (to avoid issues later on when dealing with flags).
    data["flag"] = pd.Series([flag if not pd.isnull(flag) else FLAG_OFFICIAL_DATA
                              for flag in data["flag"]], dtype="category")

    # Some datasets (at least faostat_fa) use "recipient_country" instead of "area". For consistency, change this.
    data = data.rename(columns={"area": "fao_country", "recipient_country": "fao_country",
                                "recipient_country_code": "area_code"})

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
    data = remove_duplicates(data=data, index_columns=["area_code", "year", "item_code", "element_code"],
                             verbose=True)

    # Add column for population; when creating region aggregates, this column will have the population of the countries
    # for which there was data. For example, for Europe in a specific year, the population may differ from item to item,
    # because for one item we may have more European countries informed than for the other.
    data = add_population(df=data, population_col="population_with_data", warn_on_missing_countries=False)

    # Convert back to categorical columns (maybe this should be handled automatically in `add_population_to_dataframe`)
    data = data.astype({"country": "category"})

    return data


def optimize_table_dtypes(table):
    dtypes = {
        c: "category" for c in ["area_code", "item_code", "element_code"] if c in table.columns
    }
    # NOTE: setting `.astype` in a loop over columns is slow, it is better to use
    # map all columns at once or call `repack_frame` with dtypes arg
    return catalog.frames.repack_frame(table, dtypes=dtypes)


def prepare_long_table(data: pd.DataFrame):
    # Create new table with long data.
    data_table_long = catalog.Table(data)

    # Ensure table has the optimal dtypes before storing it as feather file.
    data_table_long = optimize_table_dtypes(table=data_table_long)

    # Set appropriate indexes.
    index_columns = ["area_code", "year", "item_code", "element_code"]
    data_table_long = data_table_long.set_index(index_columns, verify_integrity=True).sort_index()

    return data_table_long


def prepare_wide_table(data: pd.DataFrame, dataset_title: str) -> catalog.Table:
    """Flatten a long table to obtain a wide table with ["country", "year"] as index.

    The input table will be pivoted to have [country, year] as index, and as many columns as combinations of
    item-element-unit entities.

    Parameters
    ----------
    data : pd.DataFrame
        Data for current domain.
    dataset_title : str
        Title for the dataset of current domain (only needed to include it in the name of the new variables).

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
    data["variable_name"] = dataframes.apply_on_categoricals(
        [data.item, data.item_code, data.element, data.element_code, data.unit],
        lambda item, item_code, element, element_code, unit:
        f"{dataset_title} || {item} | {item_code} || {element} | {element_code} || {unit}"
    )

    # Construct a human-readable variable display name (which will be shown in grapher charts).
    data['variable_display_name'] = dataframes.apply_on_categoricals(
        [data.item, data.element, data.unit], lambda item, element, unit: f"{item} - {element} ({unit})")

    # Construct a human-readable variable description (for the variable metadata).
    data['variable_description'] = dataframes.apply_on_categoricals(
        [data.item_description, data.element_description],
        lambda item_desc, element_desc: f"{item_desc}\n{element_desc}".lstrip().rstrip())

    # Pivot over long dataframe to generate a wide dataframe with country-year as index, and as many columns as
    # unique elements in "variable_name" (which should be as many as combinations of item-elements).
    # Note: We include area_code in the index for completeness, but by construction country-year should not have
    # duplicates.
    # Note: `pivot` operation is usually faster on categorical columns
    log.info("prepare_wide_table.pivot", shape=data.shape)
    # Create a wide table with just the data values.
    wide_table = catalog.Table(
        data.pivot(index=["area_code", "country", "year"], columns=["variable_name"], values="value")
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
    wide_table = catalog.utils.underscore_table(wide_table)

    return wide_table


def _variable_name_map(data: pd.DataFrame, column: str) -> Dict[str, str]:
    """Extract map {variable name -> column} from dataframe and make sure it is unique (one variable
    does not map to two distinct values)."""
    pivot = data.dropna(subset=[column]).groupby(["variable_name"], observed=True)[column].apply(set)
    assert all(pivot.map(len) == 1)
    return pivot.map(lambda x: list(x)[0]).to_dict()


def run(dest_dir: str) -> None:
    ####################################################################################################################
    # Common definitions.
    ####################################################################################################################

    # Assume dest_dir is a path to the step to be run, e.g. "faostat_qcl", and get the dataset short name from it.
    dataset_short_name = Path(dest_dir).name
    # Path to latest dataset in meadow for current FAOSTAT domain.
    meadow_data_dir = sorted((DATA_DIR / "meadow" / NAMESPACE).glob(f"*/{dataset_short_name}"))[-1].parent /\
        dataset_short_name
    # Path to dataset of FAOSTAT metadata.
    garden_metadata_dir = DATA_DIR / "garden" / NAMESPACE / VERSION / f"{NAMESPACE}_metadata"

    ####################################################################################################################
    # Load data.
    ####################################################################################################################

    # Load meadow dataset and keep its metadata.
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

    ####################################################################################################################
    # Process data.
    ####################################################################################################################

    # Harmonize items and elements, and clean data.
    data = harmonize_items(df=data, dataset_short_name=dataset_short_name)
    data = harmonize_elements(df=data)

    # Prepare data.
    data = clean_data(data=data, items_metadata=items_metadata, elements_metadata=elements_metadata,
                      countries_metadata=countries_metadata)

    # Add data for aggregate regions.
    data = add_regions(data=data, elements_metadata=elements_metadata)

    # Add per-capita variables.
    data = add_per_capita_variables(data=data, elements_metadata=elements_metadata)

    # Remove outliers (this step needs to happen after creating regions and per capita variables).
    data = remove_outliers(data)

    # TODO: Run more sanity checks (i.e. compare with previous version of the same domain).

    # Create a long table (with item code and element code as part of the index).
    data_table_long = prepare_long_table(data=data)

    # Create a wide table (with only country and year as index).
    data_table_wide = prepare_wide_table(data=data, dataset_title=datasets_metadata["owid_dataset_title"].item())

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
