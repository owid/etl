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
* Other flags (namel "B", in rl dataset and "w" in rt dataset) were not found either in the additional metadata or in
  the website definitions. They have been assigned the description "Unknown flag".
* Unfortunately, flags do not remove all ambiguities: remaining duplicates are dropped without any meaningful criterion.

"""

import json
import warnings
from copy import deepcopy
from pathlib import Path
from typing import List, cast

import numpy as np
import pandas as pd
from owid import catalog
from owid.datautils import dataframes, geo
from tqdm.auto import tqdm

from etl.paths import DATA_DIR, STEP_DIR


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

# Regions to add to the data.
# When creating region aggregates for a certain variable in a certain year, some mandatory countries must be
# informed, otherwise the aggregate will be nan (since we consider that there is not enough information).
# * A country will be considered mandatory if they exceed "min_frac_individual_population", the minimum fraction of
#   the total population of the region.
# * A country will be considered mandatory if the sum of the population of all countries (sorted by decreasing
#   population until reaching this country) exceeds "min_frac_cumulative_population", the fraction of the total
#   population of the region.
# TODO: Decide about this fraction, it seems FAO creates region aggregates even when only a few countries are informed.
#  However those informed countries may be the only relevant ones for that particular item (even if they
#  have low population).
MIN_FRAC_INDIVIDUAL_POPULATION = 0
MIN_FRAC_CUMULATIVE_POPULATION = 0.3
# Reference year to build the list of mandatory countries.
REFERENCE_YEAR = 2018
REGIONS_TO_ADD = {
    "North America": {
        "area_code": "OWID_NAM",
        "min_frac_individual_population": MIN_FRAC_INDIVIDUAL_POPULATION,
        "min_frac_cumulative_population": MIN_FRAC_CUMULATIVE_POPULATION,
    },
    "South America": {
        "area_code": "OWID_SAM",
        "min_frac_individual_population": MIN_FRAC_INDIVIDUAL_POPULATION,
        "min_frac_cumulative_population": MIN_FRAC_CUMULATIVE_POPULATION,
    },
    "Europe": {
        "area_code": "OWID_EUR",
        "min_frac_individual_population": MIN_FRAC_INDIVIDUAL_POPULATION,
        "min_frac_cumulative_population": MIN_FRAC_CUMULATIVE_POPULATION,
    },
    "European Union (27)": {
        "area_code": "OWID_EU27",
        "min_frac_individual_population": MIN_FRAC_INDIVIDUAL_POPULATION,
        "min_frac_cumulative_population": MIN_FRAC_CUMULATIVE_POPULATION,
    },
    "Africa": {
        "area_code": "OWID_AFR",
        "min_frac_individual_population": MIN_FRAC_INDIVIDUAL_POPULATION,
        "min_frac_cumulative_population": MIN_FRAC_CUMULATIVE_POPULATION,
    },
    "Asia": {
        "area_code": "OWID_ASI",
        "min_frac_individual_population": MIN_FRAC_INDIVIDUAL_POPULATION,
        "min_frac_cumulative_population": MIN_FRAC_CUMULATIVE_POPULATION,
    },
    "Oceania": {
        "area_code": "OWID_OCE",
        "min_frac_individual_population": MIN_FRAC_INDIVIDUAL_POPULATION,
        "min_frac_cumulative_population": MIN_FRAC_CUMULATIVE_POPULATION,
    },
    "Low-income countries": {
        "area_code": "OWID_LIC",
        "min_frac_individual_population": MIN_FRAC_INDIVIDUAL_POPULATION,
        "min_frac_cumulative_population": MIN_FRAC_CUMULATIVE_POPULATION,
    },
    "Upper-middle-income countries": {
        "area_code": "OWID_UMC",
        "min_frac_individual_population": MIN_FRAC_INDIVIDUAL_POPULATION,
        "min_frac_cumulative_population": MIN_FRAC_CUMULATIVE_POPULATION,
    },
    "Lower-middle-income countries": {
        "area_code": "OWID_LMC",
        "min_frac_individual_population": MIN_FRAC_INDIVIDUAL_POPULATION,
        "min_frac_cumulative_population": MIN_FRAC_CUMULATIVE_POPULATION,
    },
    "High-income countries": {
        "area_code": "OWID_HIC",
        "min_frac_individual_population": MIN_FRAC_INDIVIDUAL_POPULATION,
        "min_frac_cumulative_population": MIN_FRAC_CUMULATIVE_POPULATION,
    },
}

# When creating region aggregates, we need to ignore regions that are historical (i.e. they do not exist today as
# countries) or are geographical regions (instead of countries). We ignore them to avoid the risk of counting the
# contribution of certain countries twice.
REGIONS_TO_IGNORE_IN_AGGREGATES = [
    'Czechoslovakia',  # Historic
    'Eritrea and Ethiopia',  # Historic
    'French Southern Territories',  # Region
    'Melanesia',  # Region
    'Netherlands Antilles',  # Historic
    'Polynesia',  # Region
    'Serbia and Montenegro',  # Historic
    'Svalbard and Jan Mayen',  # Region
    'Timor',  # Region
    'USSR',  # Historic
    'United States Minor Outlying Islands',  # Region
    'Yugoslavia',  # Historic
]
# When creating region aggregates, we need to know if we are counting certain regions twice.
# This could happen if, for example, we have data for Gibraltar and United Kingdom, and we sum them to generate data
# for Europe.
# TODO: It is possible that FAO is not including, e.g. the contribution of Gibraltar in United Kingdom's data.
#  Confirm whether the following regions on the left are indeed included in the data of the country on the right.
TERRITORIES_OF_COUNTRIES = {
    'Finland': [
        'Aland Islands'
    ],
    'New Zealand': [
        'Tokelau'
    ],
    'Costa Rica': [
        'Cocos Islands'
    ],
    'Netherlands': [
        'Bonaire Sint Eustatius and Saba',
        'Sint Maarten (Dutch part)'
    ],
    'United Kingdom': [
        'Bermuda',
        'British Virgin Islands',
        'Cayman Islands',
        'Channel Islands',
        'Falkland Islands',
        'Gibraltar',
        'Guernsey',
        'Isle of Man',
        'Montserrat',
        'Pitcairn',
        'Saint Helena',
        'South Georgia and the South Sandwich Islands',
        'Turks and Caicos Islands'
    ],
    'France': [
        'French Guiana',
        'French Polynesia',
        'Guadeloupe',
        'Martinique',
        'Mayotte',
        'New Caledonia',
        'Reunion',
        'Saint Barthlemy',
        'Saint Pierre and Miquelon',
        'Saint Martin (French part)',
        'Wallis and Futuna'
    ],
    'Denmark': [
        'Faeroe Islands'
    ],
    'Australia': [
        'Christmas Island',
        'Heard Island and McDonald Islands',
        'Norfolk Island'
    ],
    'United States': [
        'Guam',
        'Northern Mariana Islands',
        'United States Virgin Islands'
    ]
}

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


def remove_rows_with_nan_value(
    data: pd.DataFrame, verbose: bool = False
) -> pd.DataFrame:
    """Remove rows for which column "value" is nan.

    Parameters
    ----------
    data : pd.DataFrame
        Data for current dataset.
    verbose : bool
        True to print information about the number and fraction of rows removed.

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
            print(
                f"Removing {n_rows_with_nan_value} rows ({frac_nan_rows: .2%}) "
                f"with nan in column 'value'."
            )
        if frac_nan_rows > 0.15:
            warnings.warn(f"{frac_nan_rows: .0%} rows of nan values removed.")
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
        True to print information about the removal of columns with nan values.

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
            print(
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
        True to print a summary of the removed duplicates.

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
            print(
                f"Removing {n_ambiguous_indexes} ambiguous indexes ({frac_ambiguous: .2%})."
            )
            print(
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


def _load_countries_regions() -> pd.DataFrame:
    # Load dataset of countries and regions.
    countries_regions = catalog.find("countries_regions", dataset="reference", namespace="owid").load()

    countries_regions = remove_regions_from_countries_regions_members(
        countries_regions, regions_to_remove=REGIONS_TO_IGNORE_IN_AGGREGATES)

    return cast(pd.DataFrame, countries_regions)


def _list_countries_in_region(region, countries_regions):
    # Number of attempts to fetch countries regions data.
    attempts = 5
    attempt = 0
    countries_in_region = None
    while attempt < attempts:
        try:
            # List countries in region.
            countries_in_region = geo.list_countries_in_region(region=region, countries_regions=countries_regions)
            break
        except ConnectionResetError:
            attempt += 1
        finally:
            assert countries_in_region is not None, "Unable to fetch countries-regions data."

    return countries_in_region


def _list_countries_in_region_that_must_have_data(region, min_frac_individual_population,
                                                  min_frac_cumulative_population, countries_regions):
    # List of countries that are part of another country (and should be removed to avoid double counting).
    subregions_to_remove = sum(list(TERRITORIES_OF_COUNTRIES.values()), [])
    # Remove those countries from countries_regions.
    # NOTE: A more sophisticated approach would be to see what countries are in the data, and, if only subregions
    #  were given (but not the containing region), then the containing regions should be removed instead. But this
    #  scenario is unlikely, and would overcomplicate things.
    countries_regions_without_subregions = remove_regions_from_countries_regions_members(
        countries_regions=countries_regions, regions_to_remove=subregions_to_remove)

    # Number of attempts to fetch countries regions data.
    attempts = 5
    attempt = 0
    countries_that_must_have_data = None
    while attempt < attempts:
        try:
            # List countries that should present in the data (since they are expected to contribute the most).
            countries_that_must_have_data = geo.list_countries_in_region_that_must_have_data(
                region=region,
                min_frac_individual_population=min_frac_individual_population,
                min_frac_cumulative_population=min_frac_cumulative_population,
                reference_year=REFERENCE_YEAR,
                countries_regions=countries_regions_without_subregions,
            )
            break
        except ConnectionResetError:
            attempt += 1
        finally:
            assert countries_that_must_have_data is not None, "Unable to fetch list of countries that must have data."

    return countries_that_must_have_data


def _find_subregions_to_remove_in_aggregation(countries):
    country_set = set(countries)
    subregions_to_remove = []
    for country in country_set:
        if country in TERRITORIES_OF_COUNTRIES:
            # If the bigger region (e.g. United Kingdom) is given in the data, remove subregions (e.g. Gibraltar).
            subregions_to_remove.extend(list(country_set & {territories
                                                            for territories in TERRITORIES_OF_COUNTRIES[country]}))

    return subregions_to_remove


def select_data_to_aggregate_without_repeating_subregions(data, countries_in_region, element_codes):
    # Select relevant portion of the data that will be aggregated.
    data_region = data[(data["country"].isin(countries_in_region)) &
                       (data["element_code"].isin(element_codes))]
    # Find subregions that have data for the same item-element as their corresponding region.
    regions_to_remove = dataframes.groupby_agg(
        data_region, groupby_columns=["item_code", "element_code"], aggregations={
            "country": lambda x: _find_subregions_to_remove_in_aggregation(x)}).dropna(subset="country")
    regions_to_remove_mask = [len(regions) > 0 for regions in regions_to_remove["country"]]
    regions_to_remove = regions_to_remove[regions_to_remove_mask].to_dict()["country"]

    # Find indexes of the previous item-element-region that have to be removed, and then remove those indexes.
    indexes_to_remove = []
    for item_element, countries_to_remove in regions_to_remove.items():
        indexes_to_remove.extend(data_region[(data_region["item_code"] == item_element[0]) &
                                             (data_region["element_code"] == item_element[1]) &
                                             (data_region["country"].isin(countries_to_remove))].index.tolist())
    data_region = data_region.drop(indexes_to_remove).reset_index(drop=True)

    return data_region


def add_regions(data, aggregations):
    data = data.copy()
    # TODO: This function takes very long, it should be optimised.

    # Invert dictionary of aggregations to have the aggregation as key, and the list of element codes as value.
    aggregations_inverted = {unique_value: pd.unique([item for item, value in aggregations.items()
                                                      if value == unique_value]).tolist()
                             for unique_value in aggregations.values()}
    for region in tqdm(REGIONS_TO_ADD):
        countries_regions = _load_countries_regions()
        countries_in_region = _list_countries_in_region(region, countries_regions=countries_regions)
        countries_that_must_have_data = _list_countries_in_region_that_must_have_data(
            region=region, min_frac_individual_population=REGIONS_TO_ADD[region]["min_frac_individual_population"],
            min_frac_cumulative_population=REGIONS_TO_ADD[region]["min_frac_cumulative_population"],
            countries_regions=countries_regions,
        )
        for aggregation in aggregations_inverted:
            # List of element codes for which the same aggregate method (e.g. "sum") will be applied.
            element_codes = aggregations_inverted[aggregation]

            data_region = select_data_to_aggregate_without_repeating_subregions(
                data=data, countries_in_region=countries_in_region, element_codes=element_codes)

            if len(data_region) > 0:
                data_region = dataframes.groupby_agg(
                    df=data_region.dropna(subset="value"), groupby_columns=["year", "item_code", "element_code"],
                    aggregations={
                        "item": "first",
                        "area_code": lambda x: REGIONS_TO_ADD[region]["area_code"],
                        "value": aggregation,
                        "country": lambda x: set(countries_that_must_have_data).issubset(set(list(x))),
                        "element": "first",
                        "fao_element": "first",
                        "fao_item": "first",
                        "item_description": "first",
                        "unit": "first",
                        "unit_short_name": "first",
                        "unit_factor": "first",
                        "fao_unit": "first",
                        "element_description": "first",
                        "flag": lambda x: x if len(x) == 1 else FLAG_MULTIPLE_FLAGS,
                        "population_with_data": "sum",
                    }
                ).reset_index().dropna(subset="element")

                # Keep only rows for which mandatory countries had data.
                data_region = data_region[data_region["country"]].reset_index(drop=True)
                # Replace column used to check if most contributing countries were present by the region's name.
                data_region["country"] = region
                # Add data for current region to data.
                data = dataframes.concatenate([data[data["country"] != region], data_region], ignore_index=True)

    # Make area_code of category type (it contains integers and strings, and feather does not support object types).
    data["area_code"] = data["area_code"].astype("str").astype("category")

    # Sort conveniently.
    data = data.sort_values(["country", "year"]).reset_index(drop=True)

    return data


def clean_data(data: pd.DataFrame, items_metadata: pd.DataFrame, elements_metadata: pd.DataFrame,
               countries_file: Path) -> pd.DataFrame:
    """Process data (including harmonization of countries and regions) and prepare it for new garden dataset.

    Parameters
    ----------
    data : pd.DataFrame
        Unprocessed data for current dataset.
    countries_file : Path or str
        Path to mapping of country names.
    items_metadata : pd.DataFrame
        Items metadata (from the metadata dataset).
    elements_metadata : pd.DataFrame
        Elements metadata (from the metadata dataset).

    Returns
    -------
    data : pd.DataFrame
        Processed data, ready to be made into a table for a garden dataset.

    """
    data = data.copy()

    # Ensure column of values is numeric (transform any possible value like "<1" into a nan).
    data["value"] = pd.to_numeric(data["value"], errors="coerce")

    # Convert nan flags into "official" (to avoid issues later on when dealing with flags).
    data["flag"] = pd.Series([flag if not pd.isnull(flag) else FLAG_OFFICIAL_DATA
                              for flag in data["flag"]], dtype="category")

    # Some datasets (at least faostat_fa) use "recipient_country" instead of "area". For consistency, change this.
    if "recipient_country" in data.columns:
        data = data.rename(
            columns={"recipient_country": "area", "recipient_country_code": "area_code"}
        )

    # Ensure year column is integer (sometimes it is given as a range of years, e.g. 2013-2015).
    data["year"] = clean_year_column(data["year"])

    # Remove rows with nan value.
    data = remove_rows_with_nan_value(data)

    # Use custom names for items, elements and units (and keep original names in "fao_*" columns).
    data = add_custom_names_and_descriptions(data, items_metadata, elements_metadata)

    # Harmonize country names.
    assert countries_file.is_file(), "countries file not found."
    data = geo.harmonize_countries(
        df=data,
        countries_file=str(countries_file),
        country_col="area",
        warn_on_unused_countries=False,
    ).rename(columns={"area": "country"}).astype({"country": "category"})
    # If countries are missing in countries file, execute etl.harmonize again and update countries file.

    check_that_countries_are_well_defined(data)

    # Remove duplicated data points (if any) keeping the one with lowest ranking flag (i.e. highest priority).
    data = remove_duplicates(data=data, index_columns=["area_code", "year", "item_code", "element_code"],
                             verbose=True)

    # Add a column for population; when creating region aggregates, this column will have the population of the countries
    # for which there was data. For example, for Europe in a specific year, the population may differ from item to item,
    # because for one item we may have more European countries informed than for the other.
    data = geo.add_population_to_dataframe(df=data, population_col="population_with_data",
                                           warn_on_missing_countries=False)

    # Create a dictionary of aggregations, specifying the operation to use when creating regions.
    # These aggregations are defined in the custom_elements_and_units.csv file, and added to the metadata dataset.
    aggregations = elements_metadata[(elements_metadata["owid_aggregation"].notnull())].\
        set_index("element_code").to_dict()["owid_aggregation"]

    if len(aggregations) > 0:
        # Add data for regions.
        data = add_regions(data=data, aggregations=aggregations)
        check_that_countries_are_well_defined(data)

    return data


def optimize_table_dtypes(table):
    table = table.copy()

    # Find optimal dtypes.
    optimal_dtypes = catalog.frames.repack_frame(table).dtypes.apply(lambda x: x.name).to_dict()

    # Ensure codes are kept as category (and not as int).
    optimal_dtypes["area_code"] = "category"
    optimal_dtypes["item_code"] = "category"
    optimal_dtypes["element_code"] = "category"

    for column in table.columns:
        if table[column].dtype != optimal_dtypes[column]:
            table[column] = table[column].astype(optimal_dtypes[column])

    return table


def prepare_long_table(data: pd.DataFrame):
    # Create new table with long data.
    data_table_long = catalog.Table(data).copy()

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
    data = data.copy()

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
    data_pivot = data.pivot(index=["area_code", "country", "year"], columns=["variable_name"],
                            values=["value", "unit", "unit_short_name", "unit_factor", "variable_display_name",
                                    "variable_description"])

    # For convenience, create a dictionary for each zeroth-level multi-index.
    data_wide = {pivot_column: data_pivot[pivot_column] for pivot_column in data_pivot.columns.levels[0]}

    # Create a wide table with just the data values.
    wide_table = catalog.Table(data_wide["value"]).copy()

    # Add metadata to each new variable in the wide data table.
    for column in wide_table.columns:
        # Add variable name.
        wide_table[column].metadata.title = column

        # Add variable unit (long name).
        variable_unit = data_wide["unit"][column].dropna().unique()
        assert len(variable_unit) == 1
        wide_table[column].metadata.unit = variable_unit[0]

        # Add variable unit (short name).
        variable_unit_short_name = data_wide["unit_short_name"][column].dropna().unique()
        assert len(variable_unit_short_name) == 1
        wide_table[column].metadata.short_unit = variable_unit_short_name[0]

        # Add variable description.
        variable_description = data_wide["variable_description"][column].dropna().unique()
        assert len(variable_description) == 1
        wide_table[column].metadata.description = variable_description[0]

        # Add display parameters (for grapher).
        wide_table[column].metadata.display = {}
        # Display name.
        variable_display_name = data_wide["variable_display_name"][column].dropna().unique()
        assert len(variable_display_name) == 1
        wide_table[column].metadata.display["name"] = variable_display_name[0]
        # Unit conversion factor (if given).
        variable_unit_factor = data_wide["unit_factor"][column].dropna().unique()
        if len(variable_unit_factor) > 0:
            assert len(variable_unit_factor) == 1
            wide_table[column].metadata.display["conversionFactor"] = variable_unit_factor[0]

    # Ensure columns have the optimal dtypes, but codes are categories.
    wide_table = optimize_table_dtypes(table=wide_table.reset_index())

    # Sort columns and rows conveniently.
    wide_table = wide_table.set_index(["country", "year"], verify_integrity=True)
    wide_table = wide_table[["area_code"] + sorted([column for column in wide_table.columns if column != "area_code"])]
    wide_table = wide_table.sort_index(level=["country", "year"]).sort_index()

    # Make all column names snake_case.
    # TODO: Add vertical bar to utils.underscore_table. When done, there will be no need to rename columns.
    wide_table = catalog.utils.underscore_table(
        wide_table.rename(columns={column: column.replace("|", "_") for column in wide_table.columns}))

    return wide_table


def run(dest_dir: str) -> None:
    ####################################################################################################################
    # Common definitions.
    ####################################################################################################################

    # Assume dest_dir is a path to the step that needs to be run, e.g. "faostat_qcl", and fetch namespace and dataset
    # short name from that path.
    dataset_short_name = Path(dest_dir).name
    # namespace = dataset_short_name.split("_")[0]
    # Path to latest dataset in meadow for current FAOSTAT domain.
    meadow_data_dir = sorted((DATA_DIR / "meadow" / NAMESPACE).glob(f"*/{dataset_short_name}"))[-1].parent /\
        dataset_short_name
    # Path to countries file.
    countries_file = STEP_DIR / "data" / "garden" / NAMESPACE / VERSION / f"{NAMESPACE}.countries.json"
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

    # Load and prepare dataset, items and element-units metadata.
    datasets_metadata = pd.DataFrame(metadata["datasets"]).reset_index()
    datasets_metadata = datasets_metadata[datasets_metadata["dataset"] == dataset_short_name].reset_index(drop=True)
    items_metadata = pd.DataFrame(metadata["items"]).reset_index()
    items_metadata = items_metadata[items_metadata["dataset"] == dataset_short_name].reset_index(drop=True)
    elements_metadata = pd.DataFrame(metadata["elements"]).reset_index()
    elements_metadata = elements_metadata[elements_metadata["dataset"] == dataset_short_name].reset_index(drop=True)

    ####################################################################################################################
    # Process data.
    ####################################################################################################################

    # Harmonize items and elements, and clean data.
    data = harmonize_items(df=data, dataset_short_name=dataset_short_name)
    data = harmonize_elements(df=data)

    data = clean_data(data=data, items_metadata=items_metadata, elements_metadata=elements_metadata,
                      countries_file=countries_file)

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

    data_table_wide.metadata.title += " - Flattened table indexed by country-year."
    data_table_wide.metadata.short_name += "_flat"
    data_table_wide.metadata.primary_key = list(data_table_wide.index.names)

    # Add wide table to the dataset (no need to repack, since columns already have optimal dtypes).
    dataset_garden.add(data_table_wide, repack=False)
