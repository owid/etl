"""Utils related to geographical entities."""

import functools
import json
import warnings
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Set, TypeVar, Union, cast

import numpy as np
import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Dataset, Table, Variable
from owid.datautils.common import ExceptionFromDocstring, warn_on_list_of_entities
from owid.datautils.dataframes import groupby_agg, map_series
from owid.datautils.io.json import load_json
from structlog import get_logger

from etl.paths import DATA_DIR, LATEST_REGIONS_DATASET_PATH

# Initialize logger.
log = get_logger()

TableOrDataFrame = TypeVar("TableOrDataFrame", pd.DataFrame, Table)

# Default regions when creating region aggregates.
REGIONS = {
    # Default continents.
    "Africa": {},
    "Asia": {},
    "Europe": {},
    "North America": {},
    "Oceania": {},
    "South America": {},
    # Income groups.
    "Low-income countries": {},
    "Upper-middle-income countries": {},
    "Lower-middle-income countries": {},
    "High-income countries": {},
    # Other special regions.
    "European Union (27)": {},
    # TODO: Consider adding also the historical regions to EU (27) definition.
    # That could be done in the regions dataset, or here, by defining:
    # {"European Union (27)": {"additional_members": ["East Germany", "West Germany", "Czechoslovakia", ...]}}
}

########################################################################################################################
# DEPRECATED: Default parameters when using "auto" mode when imposing a list of countries that must be informed, when
# creating region aggregates.
# When creating region aggregates for a certain variable in a certain year, some mandatory countries must be
# informed, otherwise the aggregate will be nan (since we consider that there is not enough information).
# A country will be considered mandatory if they exceed this minimum fraction of the total population of the region.
MIN_FRAC_INDIVIDUAL_POPULATION = 0.0
# A country will be considered mandatory if the sum of the population of all countries (sorted by decreasing
# population until reaching this country) exceeds the following fraction of the total population of the region.
MIN_FRAC_CUMULATIVE_POPULATION = 0.7
# Reference year to build the list of mandatory countries.
REFERENCE_YEAR = 2018
########################################################################################################################

########################################################################################################################
# DEPRECATED: Default paths when silently loading population and income groups datasets.
# Paths to datasets used in this module. Feel free to update the versions or paths whenever there is a
# new version of the datasets.
# DATASET_POPULATION = DATA_DIR / "garden" / "demography" / "2023-03-31" / "population"
DATASET_POPULATION = DATA_DIR / "garden" / "owid" / "latest" / "key_indicators"
TNAME_KEY_INDICATORS = "population"
# Path to Key Indicators dataset
DATASET_WB_INCOME = DATA_DIR / "garden" / "wb" / "2021-07-01" / "wb_income"
TNAME_WB_INCOME = "wb_income_group"
########################################################################################################################


@functools.lru_cache
def _load_population() -> Table:
    ####################################################################################################################
    # WARNING: This function is deprecated. All datasets should be loaded using PathFinder.
    ####################################################################################################################
    log.warning(f"Dataset {DATASET_POPULATION} is silently being loaded.")
    population = Dataset(DATASET_POPULATION)[TNAME_KEY_INDICATORS]
    return population.reset_index()


@functools.lru_cache
def _load_countries_regions() -> pd.DataFrame:
    ####################################################################################################################
    # WARNING: This function is deprecated. All datasets should be loaded using PathFinder.
    ####################################################################################################################
    log.warning(f"Dataset {LATEST_REGIONS_DATASET_PATH} is silently being loaded.")
    countries_regions = Dataset(LATEST_REGIONS_DATASET_PATH)["regions"]
    return cast(pd.DataFrame, countries_regions)


@functools.lru_cache
def _load_income_groups() -> pd.DataFrame:
    ####################################################################################################################
    # WARNING: This function is deprecated. All datasets should be loaded using PathFinder.
    ####################################################################################################################
    log.warning(f"Dataset {DATASET_WB_INCOME} is silently being loaded.")
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

    ####################################################################################################################
    WARNING: This function is deprecated, use list_members_of_region instead.
    ####################################################################################################################

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
    log.warning("This function is deprecated. Use list_members_of_region instead.")

    if countries_regions is None:
        countries_regions = _load_countries_regions()

    if income_groups is None:
        income_groups = _load_income_groups().reset_index()
    income_groups_names = income_groups["income_group"].dropna().unique().tolist()  # type: ignore

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

    ####################################################################################################################
    WARNING: This function is deprecated. Currently no alternative is implemented.
    ####################################################################################################################

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

    log.warning("This function is deprecated. Currently no alternative is implemented.")

    if countries_regions is None:
        # NOTE: This should be avoided, and it will raise a warning if used.
        countries_regions = _load_countries_regions()

    if population is None:
        # NOTE: This should be avoided, and it will raise a warning if used.
        population = _load_population()

    if income_groups is None:
        # NOTE: This should be avoided, and it will raise a warning if used.
        income_groups = _load_income_groups().reset_index()

    # List all countries in the selected region.
    members = list_countries_in_region(region, countries_regions=countries_regions, income_groups=income_groups)

    # Select population data for reference year for all countries in the region.
    reference = (
        population[(population["country"].isin(members)) & (population["year"] == reference_year)]  # type: ignore
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
    df: TableOrDataFrame,
    region: str,
    countries_in_region: Optional[List[str]] = None,
    countries_that_must_have_data: Optional[Union[List[str], Literal["auto"]]] = None,
    num_allowed_nans_per_year: Optional[int] = None,
    frac_allowed_nans_per_year: Optional[float] = None,
    min_num_values_per_year: Optional[int] = None,
    country_col: str = "country",
    year_col: str = "year",
    aggregations: Optional[Dict[str, Any]] = None,
    keep_original_region_with_suffix: Optional[str] = None,
    population: Optional[pd.DataFrame] = None,
) -> TableOrDataFrame:
    """Add aggregate data for a specific region (e.g. a continent or an income group) to a table.

    ####################################################################################################################
    WARNING: Consider using add_regions_to_table instead.
    This function is not deprecated, as it is used by add_regions_to_table, but it should not be used directly.
    ####################################################################################################################

    If data for a region already exists:
    * If keep_original_region_with_suffix is None, the original data for the region will be replaced by a new aggregate.
    * If keep_original_region_with_suffix is not None, the original data for the region will be kept, and the value of
      keep_original_region_with_suffix will be appended to the name of the region.

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
    df : TableOrDataFrame
        Original table, which may already contain data for the region.
    region : str
        Region to add.
    countries_in_region : list or None
        List of countries that are members of this region. None to load them from countries-regions dataset.
    countries_that_must_have_data : list or None or str
        * If a list of countries is passed, those countries must have data for a particular variable and year. If any of
          those countries is not informed on a particular variable and year, the region will have nan for that particular
          variable and year.
        * If "auto", a list of countries that must have data is automatically generated, based on population. When
          choosing this option, explicitly pass population as an argument (otherwise it will be silently loaded).
          See function list_countries_in_region_that_must_have_data for more details.
        * If None, nothing happens: An aggregate is constructed even if important countries are missing.
    num_allowed_nans_per_year : int or None
        * If a number is passed, this is the maximum number of nans that can be present in a particular variable and
          year. If that number of nans is exceeded, the aggregate will be nan.
        * If None, nothing happens: An aggregate is constructed regardless of the number of nans.
    frac_allowed_nans_per_year : float or None
        * If a number is passed, this is the maximum fraction of nans that can be present in a particular variable and
          year. If that fraction of nans is exceeded, the aggregate will be nan.
        * If None, nothing happens: An aggregate is constructed regardless of the fraction of nans.
    min_num_values_per_year : int or None
        * If a number is passed, this is the minimum number of non-nan values that must be present in a particular
          variable and year. If that number of values is not reached, the aggregate will be nan.
        * If None, nothing happens: An aggregate is constructed regardless of the number of non-nan values.
    country_col : str
        Name of country column.
    year_col : str
        Name of year column.
    aggregations : dict or None
        Aggregations to execute for each variable. If None, the contribution to each variable from each country in the
        region will be summed. Otherwise, only the variables indicated in the dictionary will be affected. All remaining
        variables will be nan.
    keep_original_region_with_suffix : str or None
        * If not None, the original data for a region will be kept, with the same name, but having suffix
          keep_original_region_with_suffix appended to its name.
        * If None, the original data for a region will be replaced by aggregate data constructed by this function.
    population : pd.DataFrame or None
        Only relevant if countries_that_must_have_data is "auto", otherwise ignored.
        * If not None, it should be the main population table from the population dataset.
        * If None, the population table will be silently loaded.

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
        countries_that_must_have_data = []
    elif countries_that_must_have_data == "auto":
        # List countries that should present in the data (since they are expected to contribute the most).
        countries_that_must_have_data = list_countries_in_region_that_must_have_data(
            region=region,
            population=population,
        )

    # If aggregations are not defined for each variable, assume 'sum'.
    fixed_columns = [country_col, year_col]
    if aggregations is None:
        aggregations = {variable: "sum" for variable in df.columns if variable not in fixed_columns}
    variables = list(aggregations)

    # Initialise dataframe of added regions, and add variables one by one to it.
    # df_region = Table({country_col: [], year_col: []}).astype(dtype={country_col: "object", year_col: "int"})
    # Select data for countries in the region.
    df_countries = df[df[country_col].isin(countries_in_region)]

    df_region = groupby_agg(
        df=df_countries,
        groupby_columns=year_col,
        aggregations=dict(
            **aggregations,
            **{country_col: lambda x: set(countries_that_must_have_data).issubset(set(list(x)))},
        ),
        num_allowed_nans=num_allowed_nans_per_year,
        frac_allowed_nans=frac_allowed_nans_per_year,
        min_num_values=min_num_values_per_year,
    ).reset_index()

    # Create filter that detects rows where the most contributing countries are not present.
    if df_region[country_col].dtypes == "category":
        # Doing df_region[country_col].any() fails if the country column is categorical.
        mask_countries_present = ~(df_region[country_col].astype(str))
    else:
        mask_countries_present = ~df_region[country_col]
    if mask_countries_present.any():
        # Make nan all aggregates if the most contributing countries were not present.
        df_region.loc[mask_countries_present, variables] = np.nan
    # Replace the column that was used to check if most contributing countries were present by the region's name.
    df_region[country_col] = region

    if isinstance(keep_original_region_with_suffix, str):
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
        # Remove rows in the original table containing rows for region, and append new rows for region.
        df_updated = pd.concat([df[~(df[country_col] == region)], df_region], ignore_index=True)
        # WARNING: When an aggregate is added (e.g. "Europe") just for one of the columns (and no aggregation is
        # specified for the rest of columns) and there was already data for that region, the data for the rest of
        # columns is deleted for that particular region (in the following line).
        # This is an unusual scenario, because you would normally want to replace all data for a certain region, not
        # just certain columns. However, the expected behavior would be to just replace the region data for the
        # specified column.
        # For now, simply warn that the original data for the region for those columns was deleted.
        columns_without_aggregate = set(df.drop(columns=fixed_columns).columns) - set(aggregations)
        if (len(columns_without_aggregate) > 0) and (len(df[df[country_col] == region]) > 0):
            log.warning(
                f"Region {region} already has data for columns that do not have a defined aggregation method: "
                f"({columns_without_aggregate}). That data will become nan."
            )

    # Sort conveniently.
    df_updated = df_updated.sort_values([country_col, year_col]).reset_index(drop=True)

    # If the original was Table, copy metadata
    if isinstance(df, Table):
        return Table(df_updated).copy_metadata(df)
    else:
        return df_updated  # type: ignore


def harmonize_countries(
    df: TableOrDataFrame,
    countries_file: Union[Path, str],
    excluded_countries_file: Optional[Union[Path, str]] = None,
    country_col: str = "country",
    warn_on_missing_countries: bool = True,
    make_missing_countries_nan: bool = False,
    warn_on_unused_countries: bool = True,
    warn_on_unknown_excluded_countries: bool = True,
    show_full_warning: bool = True,
) -> TableOrDataFrame:
    """Harmonize country names in dataframe, following the mapping given in a file.

    Countries in dataframe that are not in mapping will left unchanged (or converted to nan, if
    make_missing_countries_nan is True). If excluded_countries_file is given, countries in that list will be removed
    from the output data.

    Parameters
    ----------
    df : pd.DataFrame
        Original dataframe that contains a column of non-harmonized country names.
    countries_file : str
        Path to json file containing a mapping from non-harmonized to harmonized country names.
    excluded_countries_file : str
        Path to json file containing a list of non-harmonized country names to be ignored (i.e. they will not be
        harmonized, and will therefore not be included in the output data).
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
    warn_on_unknown_excluded_countries : bool
        True to warn about countries that appear in the list of non-harmonized countries to ignore, but are not found in
        the data.
    show_full_warning : bool
        True to display list of countries in warning messages.

    Returns
    -------
    df_harmonized : pd.DataFrame
        Original dataframe after standardizing the column of country names.

    """
    df_harmonized = df.copy(deep=False)

    # Load country mappings.
    countries = load_json(countries_file, warn_on_duplicated_keys=True)

    if excluded_countries_file is not None:
        # Load list of excluded countries.
        excluded_countries = load_json(excluded_countries_file, warn_on_duplicated_keys=True)

        # Check that all countries to be excluded exist in the data.
        unknown_excluded_countries = set(excluded_countries) - set(df[country_col])
        if warn_on_unknown_excluded_countries and (len(unknown_excluded_countries) > 0):
            warn_on_list_of_entities(
                list_of_entities=unknown_excluded_countries,
                warning_message="Unknown country names in excluded countries file:",
                show_list=show_full_warning,
            )

        # Remove rows corresponding to countries to be excluded.
        df_harmonized = df_harmonized[~df_harmonized[country_col].isin(excluded_countries)]

    # Harmonize all remaining country names.
    country_harmonized = map_series(
        series=df_harmonized[country_col],
        mapping=countries,
        make_unmapped_values_nan=make_missing_countries_nan,
        warn_on_missing_mappings=warn_on_missing_countries,
        warn_on_unused_mappings=warn_on_unused_countries,
        show_full_warning=show_full_warning,
    )

    # Put back metadata and add processing log.
    if isinstance(df_harmonized, Table):
        country_harmonized = Variable(
            country_harmonized, name=country_col, metadata=df_harmonized[country_col].metadata
        ).update_log(
            operation="harmonize",
        )

    df_harmonized[country_col] = country_harmonized

    return df_harmonized  # type: ignore


def add_population_to_dataframe(
    df: TableOrDataFrame,
    ds_population: Optional[Dataset] = None,
    country_col: str = "country",
    year_col: str = "year",
    population_col: str = "population",
    warn_on_missing_countries: bool = True,
    show_full_warning: bool = True,
    interpolate_missing_population: bool = False,
    expected_countries_without_population: Optional[List[str]] = None,
    _warn_deprecated: bool = True,
) -> TableOrDataFrame:
    """Add column of population to a dataframe.

    ####################################################################################################################
    WARNING: This function is deprecated. Use add_population_to_table instead.
    ####################################################################################################################

    Parameters
    ----------
    df : TableOrDataFrame
        Original dataframe that contains a column of country names and years.
    ds_population : Dataset or None
        Population dataset.
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
    interpolate_missing_population : bool
        True to linearly interpolate population on years that are presented in df, but for which we do not have
        population data; otherwise False to keep missing population data as nans.
        For example, if interpolate_missing_population is True and df has data for all years between 1900 and 1910,
        but population is only given for 1900 and 1910, population will be linearly interpolated between those years.
    expected_countries_without_population : list
        Countries that are expected to not have population (that should be ignored if warnings are activated).

    Returns
    -------
    df_with_population : pd.DataFrame
        Original dataframe after adding a column with population values.

    """
    if _warn_deprecated:
        log.warning("This function is deprecated. Use add_population_to_table instead.")

    # Load population data.
    if ds_population is not None:
        population = ds_population["population"].reset_index()
    else:
        population = _load_population()
    population = population.rename(
        columns={
            "country": country_col,
            "year": year_col,
            "population": population_col,
        }
    )[[country_col, year_col, population_col]]

    # Check if there is any unexpected missing country.
    missing_countries = set(df[country_col]) - set(population[country_col])
    if expected_countries_without_population is not None:
        missing_countries = missing_countries - set(expected_countries_without_population)
    if (len(missing_countries) > 0) and warn_on_missing_countries:
        warn_on_list_of_entities(
            list_of_entities=missing_countries,
            warning_message=(
                f"{len(missing_countries)} countries not found in population"
                " dataset. They will remain in the dataset, but have nan"
                " population."
            ),
            show_list=show_full_warning,
        )

    if interpolate_missing_population:
        # For some countries we have population data only on certain years, e.g. 1900, 1910, etc.
        # Optionally fill missing years linearly.
        countries_in_data = df[country_col].unique()
        years_in_data = df[year_col].unique()

        population = population.set_index([country_col, year_col]).reindex(
            pd.MultiIndex.from_product([countries_in_data, years_in_data], names=[country_col, year_col])
        )

        population = population.groupby(country_col).transform(
            lambda x: x.interpolate(method="linear", limit_direction="both")
        )

    # Add population to original dataframe.
    merge = pr.merge if isinstance(df, Table) else pd.merge

    if population.index.names != [None]:
        # If population has a multiindex, we need to reset it before merging.
        population = population.reset_index()

    df_with_population = merge(df, population, on=[country_col, year_col], how="left")

    return cast(TableOrDataFrame, df_with_population)


def add_population_to_table(
    tb: Table,
    ds_population: Dataset,
    country_col: str = "country",
    year_col: str = "year",
    population_col: str = "population",
    warn_on_missing_countries: bool = True,
    show_full_warning: bool = True,
    interpolate_missing_population: bool = False,
    expected_countries_without_population: Optional[List[str]] = None,
) -> Table:
    """Add column of population to a table with metadata.

    Parameters
    ----------
    tb : Table
        Original table that contains a column of country names and years.
    ds_population : Dataset
        Population dataset.
    country_col : str
        Name of column in original table with country names.
    year_col : str
        Name of column in original table with years.
    population_col : str
        Name of new column to be created with population values.
    warn_on_missing_countries : bool
        True to warn about countries that appear in original table but not in the population dataset.
    show_full_warning : bool
        True to display list of countries in warning messages.
    interpolate_missing_population : bool
        True to linearly interpolate population on years that are presented in tb, but for which we do not have
        population data; otherwise False to keep missing population data as nans.
        For example, if interpolate_missing_population is True and tb has data for all years between 1900 and 1910,
        but population is only given for 1900 and 1910, population will be linearly interpolated between those years.
    expected_countries_without_population : list
        Countries that are expected to not have population (that should be ignored if warnings are activated).

    Returns
    -------
    tb_with_population : Table
        Original table after adding a column with population values.

    """
    # Create a dataframe with an additional population column.
    df_with_population = add_population_to_dataframe(
        df=tb,
        ds_population=ds_population,
        country_col=country_col,
        year_col=year_col,
        population_col=population_col,
        warn_on_missing_countries=warn_on_missing_countries,
        show_full_warning=show_full_warning,
        interpolate_missing_population=interpolate_missing_population,
        expected_countries_without_population=expected_countries_without_population,
        _warn_deprecated=False,
    )

    # Convert the dataframe into a table, with the metadata of the original table.
    tb_with_population = Table(df_with_population).copy_metadata(tb)

    # Add metadata to the new population column.
    # TODO: Note that this is may only be necessary because description_processing is not properly propagated.
    #  Once it is, check if
    #  tb_with_population[population_col].m.to_dict() == ds_population["population"]["population"].m.to_dict()
    #  is True. If so, the following line may not be necessary.
    tb_with_population[population_col] = tb_with_population[population_col].copy_metadata(
        ds_population["population"]["population"]
    )

    return tb_with_population


def add_gdp_to_table(
    tb: Table, ds_gdp: Dataset, country_col: str = "country", year_col: str = "year", gdp_col: str = "gdp"
) -> Table:
    """Add column of GDP to a table with metadata.

    Parameters
    ----------
    tb : Table
        Original table that contains a column of country names and years.
    ds_gdp : Dataset
        Population dataset.
    country_col : str
        Name of column in original table with country names.
    year_col : str
        Name of column in original table with years.
    gdp_col : str
        Name of new column to be created with GDP values.

    Returns
    -------
    tb_with_gdo : Table
        Original table after adding a column with GDP values.

    """
    tb_with_gdp = tb.copy()

    # Read main table from GDP dataset.
    tb_gdp = ds_gdp["maddison_gdp"].reset_index()

    # Add metadata sources and licenses to the main GDP variable.
    tb_gdp["gdp"].metadata.sources = ds_gdp.metadata.sources
    tb_gdp["gdp"].metadata.licenses = ds_gdp.metadata.licenses

    gdp_columns = {
        "country": country_col,
        "year": year_col,
        "gdp": gdp_col,
    }
    tb_gdp = tb_gdp[list(gdp_columns)].rename(columns=gdp_columns)

    # Drop rows with missing values.
    tb_gdp = tb_gdp.dropna(how="any").reset_index(drop=True)

    # Add GDP column to original table.
    tb_with_gdp = tb_with_gdp.merge(tb_gdp, on=[country_col, year_col], how="left")

    return tb_with_gdp


def create_table_of_regions_and_subregions(ds_regions: Dataset, subregion_type: str = "members") -> Table:
    # Subregion type can be "members" or "successors" (or in principle also "related").
    # Get the main table from the regions dataset.
    tb_regions = ds_regions["regions"][["name", subregion_type]]

    # Get a mapping from code to region name.
    mapping = tb_regions["name"].to_dict()

    # Convert strings of lists of members into lists of aliases.
    tb_regions[subregion_type] = [
        json.loads(member) if pd.notnull(member) else [] for member in tb_regions[subregion_type]
    ]

    # Explode list of members to have one row per member.
    tb_regions = tb_regions.explode(subregion_type).dropna()

    # Map member codes to names.
    tb_regions[subregion_type] = map_series(
        series=tb_regions[subregion_type], mapping=mapping, warn_on_missing_mappings=True
    )

    # Create a column with the list of members in each region
    tb_countries_in_region = (
        tb_regions.rename(columns={"name": "region"})
        .groupby("region", as_index=True, observed=True)
        .agg({subregion_type: list})
    )

    return tb_countries_in_region


def list_members_of_region(
    region: str,
    ds_regions: Dataset,
    ds_income_groups: Optional[Dataset] = None,
    additional_regions: Optional[List[str]] = None,
    excluded_regions: Optional[List[str]] = None,
    additional_members: Optional[List[str]] = None,
    excluded_members: Optional[List[str]] = None,
    include_historical_regions_in_income_groups: bool = False,
) -> List[str]:
    """Get countries in a region, both for known regions (e.g. "Africa") and custom ones (e.g. "Europe (excl. EU-27)").

    Parameters
    ----------
    region : str
        Region name (e.g. "Africa", or "Europe (excl. EU-27)"). If the region is a known region in ds_regions, its
        members will be listed.
    ds_regions : Dataset
        Regions dataset.
    ds_income_groups : Dataset or None
        Income groups dataset. It must be given if region is an income group.
    additional_regions: list or None
        Additional regions whose members should be included in the list.
    excluded_regions: list or None
        Regions whose members should be excluded from the list.
    additional_members : list or None
        Additional individual members to include in the list.
    excluded_members : list
        Individual members to exclude from the list.
    include_historical_regions_in_income_groups : bool
        True to include historical regions in income groups.

    Returns
    -------
    countries : list
        List of countries in the specified region.

    """
    if additional_regions is None:
        additional_regions = []
    if excluded_regions is None:
        excluded_regions = []
    if additional_members is None:
        additional_members = []
    if excluded_members is None:
        excluded_members = []

    # Get the main table from the regions dataset and create a new table that has regions and members.
    tb_countries_in_region = create_table_of_regions_and_subregions(ds_regions=ds_regions, subregion_type="members")

    if ds_income_groups is not None:
        if "wb_income_group" in ds_income_groups.table_names:
            # TODO: Remove this block once the old income groups dataset has been archived.
            # Get the main table from the income groups dataset.
            tb_income = (
                ds_income_groups["wb_income_group"].reset_index().rename(columns={"income_group": "classification"})
            )
        elif "income_groups_latest" in ds_income_groups.table_names:
            # Get the table with the current definitions of income groups.
            tb_income = ds_income_groups["income_groups_latest"].reset_index()
        else:
            raise KeyError(
                "Table 'income_groups_latest' not found. "
                "You may not be using the right version of the income groups dataset ds_income_groups."
            )

        if include_historical_regions_in_income_groups:
            # Since "income_groups_latest" does not include historical regions, optionally we take their latest
            # classification from "income_groups" and add them to df_income.
            historical_regions = ds_income_groups["income_groups"].reset_index()
            # Keep only countries that are not in "income_groups_latest".
            # NOTE: This not only includes historical regions, but also countries that don't appear in
            # "income_groups_latest", like Venezuela.
            historical_regions = historical_regions[~historical_regions["country"].isin(tb_income["country"])]
            # Keep only the latest income group classification of each historical region.
            historical_regions = (
                historical_regions.sort_values(["country", "year"], ascending=True)
                .drop_duplicates(subset="country", keep="last")
                .drop(columns="year")
                .reset_index(drop=True)
            )
            # Append historical regions to latest income group classifications.
            tb_income = pd.concat([tb_income, historical_regions], ignore_index=True)

        # Create a dataframe of countries in each income group.
        tb_countries_in_income_group = (
            tb_income.rename(columns={"classification": "region", "country": "members"})  # type: ignore
            .groupby("region", as_index=True, observed=True)
            .agg({"members": list})
        )

        # Create a dataframe of members in regions, including income groups.
        tb_countries_in_region = pd.concat([tb_countries_in_region, tb_countries_in_income_group], ignore_index=False)

    # Get list of default members for the given region, if it's known.
    if region in tb_countries_in_region.index.tolist():
        countries_set = set(tb_countries_in_region.loc[region]["members"])
    else:
        # Initialise an empty set of members.
        countries_set = set()

    # List countries from the list of regions included.
    countries_set |= set(
        sum([tb_countries_in_region.loc[region_included]["members"] for region_included in additional_regions], [])
    )

    # Remove all countries from the list of regions excluded.
    countries_set -= set(
        sum([tb_countries_in_region.loc[region_excluded]["members"] for region_excluded in excluded_regions], [])
    )

    # Add the list of individual countries to be included.
    countries_set |= set(additional_members)

    # Remove the list of individual countries to be excluded.
    countries_set -= set(excluded_members)

    # Convert set of countries into a sorted list.
    countries = sorted(countries_set)

    return countries


def detect_overlapping_regions(
    df: TableOrDataFrame,
    index_columns: List[str],
    regions_and_members: Dict[str, List[str]],
    country_col: str = "country",
    year_col: str = "year",
    ignore_overlaps_of_zeros: bool = True,
):
    """Detect years on which the data for two regions overlap, e.g. a historical region and one of its successors.

    Parameters
    ----------
    df : TableOrDataFrame
        Data (with a dummy index).
    index_columns : list
        Names of index columns.
    regions_and_members : dict
        Regions to check for overlaps. Each region must have a dictionary "regions_included", listing the subregions
        contained. If the region is historical, "regions_included" would be the list of successor countries.
    country_col : str, optional
        Name of country column (usually "country").
    year_col : str, optional
        Name of year column (usually "year").
    ignore_overlaps_of_zeros : bool, optional
        True to ignore overlaps of zeros.

    Returns
    -------
    all_overlaps : dict
        All overlaps found.

    """
    # Sum over all columns to get the total sum of each column for each country-year.
    tb_total = (
        df.groupby([country_col, year_col])
        .agg({column: "sum" for column in df.columns if column not in index_columns})
        .reset_index()
    )
    # Create a list of values that will be ignored in overlaps (usually zero or nothing).
    if ignore_overlaps_of_zeros:
        overlapping_values_to_ignore = [0]
    else:
        overlapping_values_to_ignore = []
    # List all variables in data (ignoring index columns).
    variables = [column for column in df.columns if column not in index_columns]
    # List all country names found in data.
    countries_in_data = df[country_col].unique().tolist()  # type: ignore
    # List all regions found in data.
    # TODO: Possible overlaps in custom regions are not considered here. I think it would be simple enough to include
    #   here custom regions and check for overlaps.
    regions = [country for country in list(regions_and_members) if country in countries_in_data]
    # Initialize a list that will store all overlaps found.
    all_overlaps = []
    for region in regions:
        # List members of current region.
        members = [member for member in regions_and_members[region] if member in countries_in_data]
        for member in members:
            # Select data for current region.
            region_values = (
                tb_total[tb_total[country_col] == region]
                .replace(overlapping_values_to_ignore, np.nan)
                .dropna(subset=variables, how="all")
            )
            # Select data for current member.
            member_values = (
                tb_total[tb_total[country_col] == member]
                .replace(overlapping_values_to_ignore, np.nan)
                .dropna(subset=variables, how="all")
            )
            # Concatenate both selections of data, and select duplicated rows.
            combined = pd.concat([region_values, member_values])
            # Option 1: Check if there is an overlap on the same year (even if not on the same variable).
            # overlaps = combined[combined.duplicated(subset=[year_col], keep=False)]  # type: ignore
            # Option 2: Check if there is an overlap on the same year and on the same variable.
            # Count how many non-nan values are present for each year, among the two countries considered.
            counts = combined.drop(columns=country_col).groupby(year_col, as_index=True, observed=True).count()
            overlaps = counts[counts.max(axis=1) > 1]
            if len(overlaps) > 0:
                # Define new overlap in a convenient format.
                new_overlap = {year: {region, member} for year in set(overlaps.index)}
                # Add the overlap found to the dictionary of all overlaps.
                if new_overlap not in all_overlaps:
                    all_overlaps.append(new_overlap)

    return all_overlaps


def add_regions_to_table(
    tb: TableOrDataFrame,
    ds_regions: Dataset,
    ds_income_groups: Optional[Dataset] = None,
    regions: Optional[Union[List[str], Dict[str, Any]]] = None,
    aggregations: Optional[Dict[str, str]] = None,
    num_allowed_nans_per_year: Optional[int] = None,
    frac_allowed_nans_per_year: Optional[float] = None,
    min_num_values_per_year: Optional[int] = None,
    country_col: str = "country",
    year_col: str = "year",
    keep_original_region_with_suffix: Optional[str] = None,
    check_for_region_overlaps: bool = True,
    accepted_overlaps: Optional[List[Dict[int, Set[str]]]] = None,
    ignore_overlaps_of_zeros: bool = False,
    subregion_type: str = "successors",
    countries_that_must_have_data: Optional[Dict[str, List[str]]] = None,
) -> Table:
    """Add one or more region aggregates to a table (or dataframe).

    This should be the default function to use when adding data for regions to a table (or dataframe).
    This function respects the metadata of the incoming data.

    If the original data for a region already exists:
    * If keep_original_region_with_suffix is None, the original data for the region will be replaced by a new aggregate.
    * If keep_original_region_with_suffix is not None, the original data for the region will be kept, and the value of
      keep_original_region_with_suffix will be appended to the name of the region.

    Parameters
    ----------
    tb : TableOrDataFrame
        Original data, which may or may not contain data for regions.
    ds_regions : Dataset
        Regions dataset.
    ds_income_groups : Optional[Dataset], default: None
        World Bank income groups dataset.
        * If given, aggregates for income groups may be added to the data.
        * If None, no aggregates for income groups will be added.
    regions : Optional[Union[List[str], Dict[str, Any]]], default: None
        Regions to be added.
        * If it is a list, it must contain region names of default regions or income groups.
          Example: ["Africa", "Europe", "High-income countries"]
        * If it is a dictionary, each key must be the name of a default, or custom region, and the value is another
          dictionary, that can contain any of the following keys:
          * "additional_regions": Additional regions whose members should be included in the region.
          * "excluded_regions": Regions whose members should be excluded from the region.
          * "additional_members": Additional individual members (countries) to include in the region.
          * "excluded_members": Individual members to exclude from the region.
          Example: {
            "Asia": {},  # No need to define anything, since it is a default region.
            "Asia excluding China": {  # Custom region that must be defined based on other known regions and countries.
                "additional_regions": ["Asia"],
                "excluded_members": ["China"],
                },
            }
        * If None, the default regions will be added (defined as REGIONS in etl.data_helpers.geo).
    aggregations : Optional[Dict[str, str]], default: None
        Aggregation to implement for each variable.
        * If a dictionary is given, the keys must be columns of the input data, and the values must be valid operations.
          Only the variables indicated in the dictionary will be affected. All remaining variables will have an
          aggregate value for the new regions of nan.
          Example: {"column_1": "sum", "column_2": "mean", "column_3": lambda x: some_function(x)}
          If there is a "column_4" in the data, for which no aggregation is defined, then the e.g. "Europe" will have
          only nans for "column_4".
        * If None, "sum" will be assumed to all variables.
    num_allowed_nans_per_year : Optional[int], default: None
        * If a number is passed, this is the maximum number of nans that can be present in a particular variable and
          year. If that number of nans is exceeded, the aggregate will be nan.
        * If None, an aggregate is constructed regardless of the number of nans.
    frac_allowed_nans_per_year : Optional[float], default: None
        * If a number is passed, this is the maximum fraction of nans that can be present in a particular variable and
          year. If that fraction of nans is exceeded, the aggregate will be nan.
        * If None, an aggregate is constructed regardless of the fraction of nans.
    min_num_values_per_year : Optional[int], default: None
        * If a number is passed, this is the minimum number of valid (not-nan) values that must be present in a
          particular variable and year grouped. If that number of values is not reached, the aggregate will be nan.
          However, if all values in the group are valid, the aggregate will also be valid, even if the number of values
          in the group is smaller than min_num_values_per_year.
        * If None, an aggregate is constructed regardless of the number of non-nan values.
    country_col : Optional[str], default: "country"
        Name of country column.
    year_col : Optional[str], default: "year"
        Name of year column.
    keep_original_region_with_suffix : Optional[str], default: None
        * If not None, the original data for a region will be kept, with the same name, but having suffix
          keep_original_region_with_suffix appended to its name.
          Example: If keep_original_region_with_suffix is " (WB)", then there will be rows for, e.g. "Europe (WB)", with
          the original data, and rows for "Europe", with the new aggregate data.
        * If None, the original data for a region will be replaced by new aggregate data constructed by this function.
    check_for_region_overlaps : bool, default: True
        * If True, a warning is raised if a historical region has data on the same year as any of its successors.
          TODO: For now, this function simply warns about overlaps, but does nothing else about them.
            Consider adding the option to remove the data for the historical region, or the data for the successor, at
            the moment the aggregate is created.
        * If False, any possible overlap is ignored.
    accepted_overlaps : Optional[List[Dict[int, Set[str]]]], default: None
        Only relevant if check_for_region_overlaps is True.
        * If a dictionary is passed, it must contain years as keys, and sets of overlapping countries as values.
          This is used to avoid warnings when there are known overlaps in the data that are accepted.
          Note that, if the overlaps passed here are not present in the data, a warning is also raised.
          Example: [{1991: {"Georgia", "USSR"}}, {2000: {"Some region", "Some overlapping region"}}]
        * If None, any possible overlap in the data will raise a warning.
    ignore_overlaps_of_zeros : bool, default: False
        Only relevant if check_for_region_overlaps is True.
        * If True, overlaps of values of zero are ignored. In other words, if a region and one of its successors have
          both data on the same year, and that data is zero for both, no warning is raised.
        * If False, overlaps of values of zero are not ignored.
    subregion_type : str, default: "successors"
        Only relevant if check_for_region_overlaps is True.
        * If "successors", the function will look for overlaps between historical regions and their successors.
        * If "related", the function will look for overlaps between regions and their possibly related members (e.g.
          overseas territories).
    countries_that_must_have_data : Optional[Dict[str, List[str]]], default: None
        * If a dictionary is passed, each key must be a valid region, and the value should be a list of countries that
          must have data for that region. If any of those countries is not informed on a particular variable and year,
          that region will have nan for that particular variable and year.
        * If None, an aggregate is constructed regardless of the countries missing.

    Returns
    -------
    TableOrDataFrame
        Original table (or dataframe) after adding (or replacing) aggregate data for regions.

    """
    df_with_regions = pd.DataFrame(tb).copy()

    if check_for_region_overlaps:
        # Find overlaps between regions and its members.

        if accepted_overlaps is None:
            accepted_overlaps = []

        # Create a dictionary of regions and its members.
        df_regions_and_members = create_table_of_regions_and_subregions(
            ds_regions=ds_regions, subregion_type=subregion_type
        )
        regions_and_members = df_regions_and_members[subregion_type].to_dict()

        # Assume incoming table has a dummy index (the whole function may not work otherwise).
        # Example of region_and_members:
        # {"Czechoslovakia": ["Czechia", "Slovakia"]}
        all_overlaps = detect_overlapping_regions(
            df=df_with_regions,
            regions_and_members=regions_and_members,
            country_col=country_col,
            year_col=year_col,
            index_columns=[country_col, year_col],
            ignore_overlaps_of_zeros=ignore_overlaps_of_zeros,
        )
        # Example of accepted_overlaps:
        # [{1991: {"Georgia", "USSR"}}, {2000: {"Some region", "Some overlapping region"}}]
        # Check whether all accepted overlaps are found in the data, and that there are no new unknown overlaps.
        all_overlaps_sorted = sorted(all_overlaps, key=lambda d: str(d))
        accepted_overlaps_sorted = sorted(accepted_overlaps, key=lambda d: str(d))
        if all_overlaps_sorted != accepted_overlaps_sorted:
            log.warning(
                "Either the list of accepted overlaps is not found in the data or there are unknown overlaps. "
                f"Accepted overlaps: {accepted_overlaps_sorted}.\nFound overlaps: {all_overlaps_sorted}."
            )

    if aggregations is None:
        # Create region aggregates for all columns (with a simple sum) except for index columns.
        aggregations = {column: "sum" for column in df_with_regions.columns if column not in [country_col, year_col]}

    if regions is None:
        regions = REGIONS
    elif isinstance(regions, list):
        # Assume they are known regions and they have no modifications.
        regions = {region: {} for region in regions}

    if countries_that_must_have_data:
        # If countries_that_must_have_data is neither None or [], it must be a dictionary with regions as keys.
        # Check that the dictionary has the right format.
        error = "Argument countries_that_must_have_data must be a dictionary with regions as keys."
        assert set(countries_that_must_have_data) <= set(regions), error
        # Fill missing regions with an empty list.
        countries_that_must_have_data = {
            region: countries_that_must_have_data.get(region, []) for region in list(regions)
        }
    else:
        countries_that_must_have_data = {region: [] for region in list(regions)}

    # Add region aggregates.
    for region in regions:
        # Check that the content of the region dictionary is as expected.
        expected_items = {"additional_regions", "excluded_regions", "additional_members", "excluded_members"}
        unknown_items = set(regions[region]) - expected_items
        if len(unknown_items) > 0:
            log.warning(
                f"Unknown items in dictionary of regions {region}: {unknown_items}. Expected: {expected_items}."
            )

        # List members of the region.
        members = list_members_of_region(
            region=region,
            ds_regions=ds_regions,
            ds_income_groups=ds_income_groups,
            additional_regions=regions[region].get("additional_regions"),
            excluded_regions=regions[region].get("excluded_regions"),
            additional_members=regions[region].get("additional_members"),
            excluded_members=regions[region].get("excluded_members"),
            # By default, include historical regions in income groups.
            include_historical_regions_in_income_groups=True,
        )
        # TODO: Here we could optionally define _df_with_regions, which is passed to add_region_aggregates, and is
        #   identical to df_with_regions, but overlaps in accepted_overlaps are solved (e.g. the data for the historical
        #   or parent region is made nan).

        # Add aggregate data for current region.
        df_with_regions = add_region_aggregates(
            df=df_with_regions,
            region=region,
            aggregations=aggregations,
            countries_in_region=members,
            countries_that_must_have_data=countries_that_must_have_data[region],
            num_allowed_nans_per_year=num_allowed_nans_per_year,
            frac_allowed_nans_per_year=frac_allowed_nans_per_year,
            min_num_values_per_year=min_num_values_per_year,
            country_col=country_col,
            year_col=year_col,
            keep_original_region_with_suffix=keep_original_region_with_suffix,
        )

    # If the original object was a Table, copy metadata
    if isinstance(tb, Table):
        # TODO: Add entry to processing log.
        return Table(df_with_regions).copy_metadata(tb)
    else:
        return df_with_regions  # type: ignore
