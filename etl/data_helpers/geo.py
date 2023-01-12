"""Utils related to geographical entities."""

import functools
import json
import warnings
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, cast

import numpy as np
import pandas as pd
from owid.catalog import Dataset
from owid.datautils.common import ExceptionFromDocstring, warn_on_list_of_entities
from owid.datautils.dataframes import groupby_agg, map_series
from owid.datautils.io.json import load_json

from etl.paths import DATA_DIR, REFERENCE_DATASET

# When creating region aggregates for a certain variable in a certain year, some mandatory countries must be
# informed, otherwise the aggregate will be nan (since we consider that there is not enough information).
# A country will be considered mandatory if they exceed this minimum fraction of the total population of the region.
MIN_FRAC_INDIVIDUAL_POPULATION = 0.0
# A country will be considered mandatory if the sum of the population of all countries (sorted by decreasing
# population until reaching this country) exceeds the following fraction of the total population of the region.
MIN_FRAC_CUMULATIVE_POPULATION = 0.7
# Reference year to build the list of mandatory countries.
REFERENCE_YEAR = 2018
# Maximum fraction of nans allowed per year when doing aggregations (None to allow any fraction of nans).
FRAC_ALLOWED_NANS_PER_YEAR = 0.2
# Maximum number of nans allowed per year when doing aggregations (None to allow any number of nans).
NUM_ALLOWED_NANS_PER_YEAR = None

# Paths to datasets used in this module. Feel free to update the versions or paths whenever there is a
# new version of the datasets.
# Path to Key Indicators dataset
DATASET_KEY_INDICATORS = DATA_DIR / "garden" / "owid" / "latest" / "key_indicators"
TNAME_KEY_INDICATORS = "population"
# Path to Key Indicators dataset
DATASET_WB_INCOME = DATA_DIR / "garden" / "wb" / "2021-07-01" / "wb_income"
TNAME_WB_INCOME = "wb_income_group"
# Path to Key Indicators dataset
TNAME_REFERENCE = "countries_regions"


@functools.lru_cache
def _load_population() -> pd.DataFrame:
    population = Dataset(DATASET_KEY_INDICATORS)[TNAME_KEY_INDICATORS]
    population = population.reset_index()
    return cast(pd.DataFrame, population)


@functools.lru_cache
def _load_countries_regions() -> pd.DataFrame:
    countries_regions = Dataset(REFERENCE_DATASET)[TNAME_REFERENCE]
    return cast(pd.DataFrame, countries_regions)


@functools.lru_cache
def _load_income_groups() -> pd.DataFrame:
    income_groups = Dataset(DATASET_WB_INCOME)[TNAME_WB_INCOME]
    return cast(pd.DataFrame, income_groups)


class RegionNotFound(ExceptionFromDocstring):
    """Region was not found in countries-regions dataset."""


def list_countries_in_region(
    region: str,
    countries_regions: Optional[pd.DataFrame] = None,
    income_groups: Optional[pd.DataFrame] = None,
) -> List[str]:
    """List countries that are members of a region.

    Parameters
    ----------
    region : str
        Name of the region (e.g. Europe).
    countries_regions : pd.DataFrame or None
        Countries-regions dataset, or None to load it from the catalog.
    income_groups : pd.DataFrame or None
        Income-groups dataset, or None, to load it from the catalog.

    Returns
    -------
    members : list
        Names of countries that are members of the region.

    """
    if countries_regions is None:
        countries_regions = _load_countries_regions()

    # TODO: Remove lines related to income_groups once they are included in countries-regions dataset.
    if income_groups is None:
        income_groups = _load_income_groups().reset_index()
    income_groups_names = income_groups["income_group"].dropna().unique().tolist()  # type: ignore

    # TODO: Once countries-regions has additional columns 'is_historic' and 'is_country', select only countries, and not
    #  historical regions.
    if region in countries_regions["name"].tolist():
        # Find codes of member countries in this region.
        member_codes_str = countries_regions[countries_regions["name"] == region]["members"].item()
        if pd.isnull(member_codes_str):
            member_codes = []
        else:
            member_codes = json.loads(member_codes_str)
        # Get harmonized names of these countries.
        members = countries_regions.loc[member_codes]["name"].tolist()  # type: List[str]
    elif region in income_groups_names:
        members = income_groups[income_groups["income_group"] == region]["country"].unique().tolist()  # type: ignore
    else:
        raise RegionNotFound

    return members


def list_countries_in_region_that_must_have_data(
    region: str,
    reference_year: int = REFERENCE_YEAR,
    min_frac_individual_population: float = MIN_FRAC_INDIVIDUAL_POPULATION,
    min_frac_cumulative_population: float = MIN_FRAC_CUMULATIVE_POPULATION,
    countries_regions: Optional[pd.DataFrame] = None,
    income_groups: Optional[pd.DataFrame] = None,
    population: Optional[pd.DataFrame] = None,
    verbose: bool = False,
) -> List[str]:
    """List countries of a region that are expected to have the largest contribution to any variable.

    The contribution of each country is based on their population relative to the region's total.

    Method to select countries:
    1. Select countries whose population is, on a certain reference year (reference_year), larger than a fraction of
    min_frac_individual_population with respect to the total population of the region.
    2. Among those, sort countries by descending population, and cut as soon as the cumulative population exceeds
    min_frac_cumulative_population.
    Note: It may not be possible to fulfil both conditions. In that case, a warning is raised.

    Parameters
    ----------
    region : str
        Name of the region.
    reference_year : int
        Reference year to consider when selecting countries.
    min_frac_individual_population : float
        Minimum fraction of the total population of the region that each of the listed countries must exceed.
    min_frac_cumulative_population : float
        Minimum fraction of the total population of the region that the sum of the listed countries must exceed.
    countries_regions : pd.DataFrame or None
        Countries-regions dataset, or None, to load it from owid catalog.
    income_groups : pd.DataFrame or None
        Income-groups dataset, or None, to load it from the catalog.
    population : pd.DataFrame or None
        Population dataset, or None, to load it from owid catalog.
    verbose : bool
        True to print the number of countries (and percentage of cumulative population) that must have data.

    Returns
    -------
    countries : list
        Countries that are expected to have the largest contribution.

    """
    if countries_regions is None:
        countries_regions = _load_countries_regions()

    if population is None:
        population = _load_population()

    if income_groups is None:
        income_groups = _load_income_groups().reset_index()

    # List all countries in the selected region.
    members = list_countries_in_region(region, countries_regions=countries_regions, income_groups=income_groups)

    # Select population data for reference year for all countries in the region.
    reference = (
        population[(population["country"].isin(members)) & (population["year"] == reference_year)]
        .dropna(subset="population")
        .sort_values("population", ascending=False)
        .reset_index(drop=True)
    )

    # Calculate total population in the region, and the fractional contribution of each country.
    total_population = reference["population"].sum()
    reference["fraction"] = reference["population"] / total_population

    # Select countries that exceed a minimum individual fraction of the total population of the region.
    selected = reference[(reference["fraction"] > min_frac_individual_population)].reset_index(drop=True)

    # Among remaining countries, select countries that, combined, exceed a minimum fraction of the total population.
    selected["cumulative_fraction"] = selected["population"].cumsum() / total_population
    candidates_to_ignore = selected[selected["cumulative_fraction"] > min_frac_cumulative_population]
    if len(candidates_to_ignore) > 0:
        selected = selected.loc[0 : candidates_to_ignore.index[0]]

    if (min_frac_individual_population == 0) and (min_frac_cumulative_population == 0):
        selected = pd.DataFrame({"country": [], "fraction": []})
    elif (len(selected) == 0) or ((len(selected) == len(reference)) and (len(reference) > 1)):
        # This happens when the only way to fulfil the conditions is to include all countries.
        warnings.warn("Conditions are too strict to select countries that must be included in the data.")
        selected = reference.copy()

    if verbose:
        print(
            f"{len(selected)} countries must be informed for {region} (covering"
            f" {selected['fraction'].sum() * 100: .2f}% of the population; otherwise"
            " aggregate data will be nan."
        )
    countries = selected["country"].tolist()  # type: List[str]

    return countries


def add_region_aggregates(
    df: pd.DataFrame,
    region: str,
    countries_in_region: Optional[List[str]] = None,
    countries_that_must_have_data: Optional[List[str]] = None,
    num_allowed_nans_per_year: Union[int, None] = NUM_ALLOWED_NANS_PER_YEAR,
    frac_allowed_nans_per_year: Union[float, None] = FRAC_ALLOWED_NANS_PER_YEAR,
    country_col: str = "country",
    year_col: str = "year",
    aggregations: Optional[Dict[str, Any]] = None,
    keep_original_region_with_suffix: Optional[str] = None,
) -> pd.DataFrame:
    """Add data for regions (e.g. income groups or continents) to a dataset.

    If data for a region already exists in the dataset, it will be replaced.

    When adding up the contribution from different countries (e.g. Spain, France, etc.) of a region (e.g. Europe), we
    want to avoid two problems:
    * Generating a series of nan, because one small country (with a negligible contribution) has nans.
    * Generating a series that underestimates the real one, because of treating missing values as zeros.

    To avoid these problems, we first define a list of "big countries" that must be present in the data, in order to
    safely do the aggregation. If any of these countries is not present for a particular variable and year, the
    aggregation will be nan for that variable and year. Otherwise, if all big countries are present, any other missing
    country will be assumed to have zero contribution to the variable.
    For example, when aggregating the electricity demand of North America, United States and Mexico cannot be missing,
    because otherwise the aggregation would significantly underestimate the true electricity demand of North America.

    Additionally, the aggregation of a particular variable for a particular year cannot have too many nans. If the
    number of nans exceeds num_allowed_nans_per_year, or if the fraction of nans exceeds frac_allowed_nans_per_year, the
    aggregation for that variable and year will be nan.

    Parameters
    ----------
    df : pd.Dataframe
        Original dataset, which may contain data for that region (in which case, it will be replaced by the ).
    region : str
        Region to add.
    countries_in_region : list or None
        List of countries that are members of this region. None to load them from countries-regions dataset.
    countries_that_must_have_data : list or None
        List of countries that must have data for a particular variable and year, otherwise the region will have nan for
        that particular variable and year. See function list_countries_in_region_that_must_have_data for more
        details.
    num_allowed_nans_per_year : int or None
        Maximum number of nans that can be present in a particular variable and year. If exceeded, the aggregation will
        be nan.
    frac_allowed_nans_per_year : float or None
        Maximum fraction of nans that can be present in a particular variable and year. If exceeded, the aggregation
        will be nan.
    country_col : str
        Name of country column.
    year_col : str
        Name of year column.
    aggregations : dict or None
        Aggregations to execute for each variable. If None, the contribution to each variable from each country in the
        region will be summed. Otherwise, only the variables indicated in the dictionary will be affected. All remaining
        variables will be nan.
    keep_original_region_with_suffix : str or None
        If None, original data for region will be replaced by aggregate data constructed by this function. If not None,
        original data for region will be kept, with the same name, but having suffix keep_original_region_with_suffix
        added to its name.

    Returns
    -------
    df_updated : pd.DataFrame
        Original dataset after adding (or replacing) data for selected region.

    """
    if countries_in_region is None:
        # List countries in the region.
        countries_in_region = list_countries_in_region(
            region=region,
        )

    if countries_that_must_have_data is None:
        # List countries that should present in the data (since they are expected to contribute the most).
        countries_that_must_have_data = list_countries_in_region_that_must_have_data(
            region=region,
        )

    # If aggregations are not defined for each variable, assume 'sum'.
    fixed_columns = [country_col, year_col]
    if aggregations is None:
        aggregations = {variable: "sum" for variable in df.columns if variable not in fixed_columns}
    variables = list(aggregations)

    # Initialise dataframe of added regions, and add variables one by one to it.
    df_region = pd.DataFrame({country_col: [], year_col: []}).astype(dtype={country_col: "object", year_col: "int"})
    # Select data for countries in the region.
    df_countries = df[df[country_col].isin(countries_in_region)]
    for variable in variables:
        df_added = groupby_agg(
            df=df_countries,
            groupby_columns=year_col,
            aggregations={
                country_col: lambda x: set(countries_that_must_have_data).issubset(set(list(x))),
                variable: aggregations[variable],
            },
            num_allowed_nans=num_allowed_nans_per_year,
            frac_allowed_nans=frac_allowed_nans_per_year,
        ).reset_index()
        # Make nan all aggregates if the most contributing countries were not present.
        df_added.loc[~df_added[country_col], variable] = np.nan
        # Replace the column that was used to check if most contributing countries were present by the region's name.
        df_added[country_col] = region
        # Include this variable to the dataframe of added regions.
        df_region = pd.merge(df_region, df_added, on=[country_col, year_col], how="outer")

    if type(keep_original_region_with_suffix) == str:
        # Keep rows in the original dataframe containing rows for region (adding a suffix to the region name), and then
        # append new rows for region.
        rows_original_region = df[country_col] == region
        df_original_region = df[rows_original_region].reset_index(drop=True)
        # Append suffix at the end of the name of the original region.
        df_original_region[country_col] = region + cast(str, keep_original_region_with_suffix)
        df_updated = pd.concat(
            [df[~rows_original_region], df_original_region, df_region],
            ignore_index=True,
        )
    else:
        # Remove rows in the original dataframe containing rows for region, and append new rows for region.
        df_updated = pd.concat([df[~(df[country_col] == region)], df_region], ignore_index=True)

    # Sort conveniently.
    df_updated = df_updated.sort_values([country_col, year_col]).reset_index(drop=True)

    return df_updated


def harmonize_countries(
    df: pd.DataFrame,
    countries_file: Union[Path, str],
    country_col: str = "country",
    warn_on_missing_countries: bool = True,
    make_missing_countries_nan: bool = False,
    warn_on_unused_countries: bool = True,
    show_full_warning: bool = True,
) -> pd.DataFrame:
    """Harmonize country names in dataframe, following the mapping given in a file.

    Parameters
    ----------
    df : pd.DataFrame
        Original dataframe that contains a column of non-harmonized country names.
    countries_file : str
        Path to json file containing a mapping from non-harmonized to harmonized country names.
    country_col : str
        Name of column in df containing non-harmonized country names.
    warn_on_missing_countries : bool
        True to warn about countries that appear in original table but not in countries file.
    make_missing_countries_nan : bool
        True to make nan any country that appears in original dataframe but not in countries file. False to keep their
        original (possibly non-harmonized) names.
    warn_on_unused_countries : bool
        True to warn about countries that appear in countries file but are useless (since they do not appear in original
        dataframe).
    show_full_warning : bool
        True to display list of countries in warning messages.

    Returns
    -------
    df_harmonized : pd.DataFrame
        Original dataframe after standardizing the column of country names.

    """
    # Load country mappings.
    countries = load_json(countries_file, warn_on_duplicated_keys=True)

    # Replace country names following the mapping given in the countries file.
    # Countries in dataframe that are not in mapping will be either left unchanged of converted to nan.
    df_harmonized = df.copy(deep=False)
    df_harmonized[country_col] = map_series(
        series=df[country_col],
        mapping=countries,
        make_unmapped_values_nan=make_missing_countries_nan,
        warn_on_missing_mappings=warn_on_missing_countries,
        warn_on_unused_mappings=warn_on_unused_countries,
        show_full_warning=show_full_warning,
    )

    return df_harmonized


def add_population_to_dataframe(
    df: pd.DataFrame,
    country_col: str = "country",
    year_col: str = "year",
    population_col: str = "population",
    warn_on_missing_countries: bool = True,
    show_full_warning: bool = True,
) -> pd.DataFrame:
    """Add column of population to a dataframe.

    Parameters
    ----------
    df : pd.DataFrame
        Original dataframe that contains a column of country names and years.
    country_col : str
        Name of column in original dataframe with country names.
    year_col : str
        Name of column in original dataframe with years.
    population_col : str
        Name of new column to be created with population values.
    warn_on_missing_countries : bool
        True to warn about countries that appear in original dataframe but not in population dataset.
    show_full_warning : bool
        True to display list of countries in warning messages.

    Returns
    -------
    df_with_population : pd.DataFrame
        Original dataframe after adding a column with population values.

    """
    # Load population data.
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
            warn_on_list_of_entities(
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
