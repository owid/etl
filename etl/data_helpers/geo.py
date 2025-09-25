"""Utils related to geographical entities."""

import functools
import json
import warnings
from datetime import datetime
from functools import cache
from pathlib import Path
from typing import Any, Hashable, Literal, TypeVar, cast

import numpy as np
import owid.catalog.processing as pr
import pandas as pd
from deprecated import deprecated
from owid.catalog import Dataset, Table, Variable
from owid.datautils.common import ExceptionFromDocstring, warn_on_list_of_entities
from owid.datautils.dataframes import groupby_agg, map_series
from owid.datautils.io.json import load_json
from structlog import get_logger

from etl.paths import DATA_DIR, LATEST_INCOME_DATASET_PATH, LATEST_POPULATION_DATASET_PATH, LATEST_REGIONS_DATASET_PATH

# Initialize logger.
log = get_logger()

TableOrDataFrame = TypeVar("TableOrDataFrame", pd.DataFrame, Table)

# Default income groups.
INCOME_GROUPS = {
    "Low-income countries": {},
    "Upper-middle-income countries": {},
    "Lower-middle-income countries": {},
    "High-income countries": {},
}
# Default regions when creating region aggregates.
REGIONS = {
    # Default continents.
    "Africa": {},
    "Asia": {},
    "Europe": {},
    "North America": {},
    "Oceania": {},
    "South America": {},
    # Other special regions.
    "European Union (27)": {},
    # TODO: Consider adding also the historical regions to EU (27) definition.
    # That could be done in the regions dataset, or here, by defining:
    # {"European Union (27)": {"additional_members": ["East Germany", "West Germany", "Czechoslovakia", ...]}}
}
# Add income groups to default regions.
REGIONS.update(INCOME_GROUPS)
# Entity codes used for income groups.
INCOME_GROUPS_ENTITY_CODES = {
    "Low-income countries": "OWID_LIC",
    "Lower-middle-income countries": "OWID_LMC",
    "Upper-middle-income countries": "OWID_UMC",
    "High-income countries": "OWID_HIC",
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
# DEPRECATED: Default paths when silently loading income groups datasets.
# Paths to datasets used in this module. Feel free to update the versions or paths whenever there is a
# new version of the datasets.
DATASET_WB_INCOME = DATA_DIR / "garden" / "wb" / "2021-07-01" / "wb_income"
TNAME_WB_INCOME = "wb_income_group"
########################################################################################################################


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
    countries_regions: pd.DataFrame | None = None,
    income_groups: pd.DataFrame | None = None,
) -> list[str]:
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
        members = countries_regions.loc[member_codes]["name"].tolist()  # type: list[str]
    elif region in income_groups_names:
        members = income_groups[income_groups["income_group"] == region]["country"].unique().tolist()  # type: ignore
    else:
        raise RegionNotFound

    return members


def list_countries_in_region_that_must_have_data(
    region: str,
    population: pd.DataFrame,
    reference_year: int = REFERENCE_YEAR,
    min_frac_individual_population: float = MIN_FRAC_INDIVIDUAL_POPULATION,
    min_frac_cumulative_population: float = MIN_FRAC_CUMULATIVE_POPULATION,
    countries_regions: pd.DataFrame | None = None,
    income_groups: pd.DataFrame | None = None,
    verbose: bool = False,
) -> list[str]:
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
    population : pd.DataFrame
        Population dataframe to load
    countries_regions : pd.DataFrame or None
        Countries-regions dataset, or None, to load it from owid catalog.
    income_groups : pd.DataFrame or None
        Income-groups dataset, or None, to load it from the catalog.
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
    countries = selected["country"].tolist()  # type: list[str]

    return countries


def add_region_aggregates(
    df: TableOrDataFrame,
    region: str,
    countries_in_region: list[str] | None = None,
    index_columns: list[str] | None = None,
    countries_that_must_have_data: list[str] | Literal["auto"] | None = None,
    frac_countries_that_must_have_data: float | None = None,
    num_allowed_nans_per_year: int | None = None,
    frac_allowed_nans_per_year: float | None = None,
    min_num_values_per_year: int | None = None,
    country_col: str = "country",
    year_col: str = "year",
    aggregations: dict[str, Any] | None = None,
    keep_original_region_with_suffix: str | None = None,
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
    index_columns : Optional[List[str]], default: None
        Names of index columns (usually ["country", "year"]). Aggregations will be done on groups defined by these
        columns (excluding the country column). A country and a year column should always be included.
        But more dimensions are also allowed, e.g. index_columns=["country", "year", "type"].
    countries_that_must_have_data : list or None or str
        * If a list of countries is passed, those countries must have data for a particular variable and year. If any of
          those countries is not informed on a particular variable and year, the region will have nan for that particular
          variable and year.
        * If None, nothing happens: An aggregate is constructed even if important countries are missing.
    frac_countries_that_must_have_data : float or None
        * If a number is passed, this is the minimum fraction of the total population of the region that must be
          represented by the countries that have data for a particular variable and year. If that fraction is not
          reached, the aggregate will be nan.
        * If None, it is assumed that a 100% fraction is required.
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

    if index_columns is None:
        index_columns = [country_col, year_col]

    # If aggregations are not defined for each variable, assume 'sum'.
    if aggregations is None:
        aggregations = {variable: "sum" for variable in df.columns if variable not in index_columns}
    variables = list(aggregations)

    # Initialise dataframe of added regions, and add variables one by one to it.
    # df_region = Table({country_col: [], year_col: []}).astype(dtype={country_col: "object", year_col: "int"})
    # Select data for countries in the region.
    df_countries = df[df[country_col].isin(countries_in_region)]

    def _check_countries_must_have_data(countries):
        # Get set of countries with data
        countries_with_data = set(list(countries))
        if frac_countries_that_must_have_data is None:
            return set(countries_that_must_have_data).issubset(countries_with_data)
        elif frac_countries_that_must_have_data > 1:
            raise ValueError(
                f"`frac_countries_that_must_have_data` must be between 0 and 1, got {frac_countries_that_must_have_data}."
            )
        else:
            # If a fraction of countries that must have data is defined, check that the fraction of countries that
            # have data is larger than the defined fraction.
            num_countries_relevant = len(countries_that_must_have_data)
            num_countries_relevant_with_data = len(set(countries_that_must_have_data).intersection(countries_with_data))
            return num_countries_relevant_with_data / num_countries_relevant >= frac_countries_that_must_have_data

    df_region = groupby_agg(
        df=df_countries,
        groupby_columns=[column for column in index_columns if column != country_col],
        aggregations=dict(
            **aggregations,
            **{country_col: lambda x: _check_countries_must_have_data(x)},
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
        dfs_to_concat = [df[~(df[country_col] == region)], df_region]
        df_updated = pd.concat([df for df in dfs_to_concat if not df.empty], ignore_index=True)
        # WARNING: When an aggregate is added (e.g. "Europe") just for one of the columns (and no aggregation is
        # specified for the rest of columns) and there was already data for that region, the data for the rest of
        # columns is deleted for that particular region (in the following line).
        # This is an unusual scenario, because you would normally want to replace all data for a certain region, not
        # just certain columns. However, the expected behavior would be to just replace the region data for the
        # specified column.
        # For now, simply warn that the original data for the region for those columns was deleted.
        columns_without_aggregate = set(df.drop(columns=index_columns).columns) - set(aggregations)
        if (len(columns_without_aggregate) > 0) and (len(df[df[country_col] == region]) > 0):
            log.warning(
                f"Region {region} already has data for columns that do not have a defined aggregation method: "
                f"({columns_without_aggregate}). That data will become nan."
            )

    # Sort conveniently.
    df_updated = df_updated.sort_values(index_columns).reset_index(drop=True)  # type: ignore

    # Convert country to categorical if the original was
    if df[country_col].dtype.name == "category":
        df_updated = df_updated.astype({country_col: "category"})

    # If the original was Table, copy metadata
    if isinstance(df, Table):
        return Table(df_updated).copy_metadata(df)
    else:
        return df_updated  # type: ignore


def harmonize_countries(
    df: TableOrDataFrame,
    countries_file: Path | str,
    excluded_countries_file: Path | str | None = None,
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


def _add_population_to_dataframe(
    df: TableOrDataFrame,
    tb_population: Table,
    country_col: str = "country",
    year_col: str = "year",
    population_col: str = "population",
    warn_on_missing_countries: bool = True,
    show_full_warning: bool = True,
    interpolate_missing_population: bool = False,
    expected_countries_without_population: list[str] | None = None,
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
    tb_population : Table
        Population table.
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
    population = tb_population.rename(
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


@deprecated("This function is deprecated. Use `etl.data_helpers.misc.interpolate_table` instead.")
def interpolate_table(
    df: TableOrDataFrame,
    country_col: str,
    time_col: str,
    all_years: bool = False,
    all_dates_per_country: bool = False,
) -> TableOrDataFrame:
    ####################################################################################################################
    # WARNING: This function is deprecated. Use `etl.data_helpers.misc.interpolate_table` instead.
    ####################################################################################################################

    """Interpolate missing values in a column linearly.

    df: Table or DataFrame
        Should contain three columns: country, year, and the column to be interpolated.
    country_col: str
        Name of the column with country names.
    year_col: str
        Name of the column with years.
    all_years: bool
        Use the complete date range (regardless of country).
    all_dates_per_country: bool
        Use the complete date range for reach country. That is, the date range for a country may differ from another. The important aspect here is that no date is skipped per country. This overrules `all_years`.
    """

    def _get_complete_date_range(ds):
        date_min = ds.min()
        date_max = ds.max()
        if isinstance(date_max, datetime):
            return pd.date_range(start=date_min, end=date_max)
        else:
            return range(date_min, date_max + 1)

    if all_dates_per_country:

        def _reindex_dates(group):
            complete_date_range = _get_complete_date_range(group["date"])
            group = group.set_index("date").reindex(complete_date_range).reset_index().rename(columns={"index": "date"})
            group[country_col] = group[country_col].ffill().bfill()  # Fill NaNs in 'country'
            return group

        # Apply the reindexing to each group
        df = df.groupby(country_col).apply(_reindex_dates).reset_index(drop=True).set_index(["country", "date"])  # type: ignore
    else:
        # For some countries we have population data only on certain years, e.g. 1900, 1910, etc.
        # Optionally fill missing years linearly.
        countries_in_data = df[country_col].unique()
        # Get list of year-country tuples
        if all_years:
            years_in_data = _get_complete_date_range(df[time_col])
        else:
            years_in_data = df[time_col].unique()
        # Reindex
        df = (
            df.set_index([country_col, time_col])
            .reindex(pd.MultiIndex.from_product([countries_in_data, years_in_data], names=[country_col, time_col]))  # type: ignore
            .sort_index()
        )

    # Interpolate
    df = (
        df.groupby(country_col)
        .transform(lambda x: x.interpolate(method="linear", limit_direction="both"))  # type: ignore
        .reset_index()
    )

    return df


def add_population_to_table(
    tb: Table,
    ds_population: Dataset,
    country_col: str = "country",
    year_col: str = "year",
    population_col: str = "population",
    warn_on_missing_countries: bool = True,
    show_full_warning: bool = True,
    interpolate_missing_population: bool = False,
    expected_countries_without_population: list[str] | None = None,
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
    # Load population
    tb_population = ds_population.read("population", safe_types=False)

    # Create a dataframe with an additional population column.
    df_with_population = _add_population_to_dataframe(
        df=tb,
        tb_population=tb_population,
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
        GDP dataset (either the old ggdc_maddison or the new maddison_project_database step).
    country_col : str
        Name of column in original table with country names.
    year_col : str
        Name of column in original table with years.
    gdp_col : str
        Name of new column to be created with GDP values.

    Returns
    -------
    tb_with_gdp : Table
        Original table after adding a column with GDP values.

    """
    tb_with_gdp = tb.copy()

    # Read main table from GDP dataset.
    error = (
        "GDP dataset is expected to have a table called either 'maddison_gdp' (old dataset) or "
        "'maddison_project_database' (new dataset)."
    )
    assert (ds_gdp.table_names == ["maddison_gdp"]) or (ds_gdp.table_names == ["maddison_project_database"]), error
    tb_gdp = ds_gdp[ds_gdp.table_names[0]].reset_index()

    # Adapt GDP table to the column naming of the original table.
    gdp_columns = {
        "country": country_col,
        "year": year_col,
        "gdp": gdp_col,
    }
    tb_gdp = tb_gdp[list(gdp_columns)].rename(columns=gdp_columns, errors="raise")

    # Drop rows with missing values in the GDP table.
    tb_gdp = tb_gdp.dropna(how="any").reset_index(drop=True)

    # Add GDP column to original table.
    tb_with_gdp = tb_with_gdp.merge(tb_gdp, on=[country_col, year_col], how="left")

    return tb_with_gdp


@cache
def create_table_of_regions_and_subregions(
    ds_regions: Dataset, subregion_type: str = "members", unpack_subregions: bool = False
) -> Table:
    # Subregion type can be "members" or "successors" (or in principle also "related").
    # Get the main table from the regions dataset.
    tb_regions = ds_regions["regions"]
    tb_regions = tb_regions.loc[:, ["name", subregion_type]]

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

    if unpack_subregions:
        # Replace subregions by their countries.
        # For example, "World" includes "Africa"; replace that by all African countries.
        tb_regions = tb_regions.reset_index()
        # For safety (to avoid an infinite loop, track the number of iterations).
        iterations = 0
        # Currently, the only case of composite subregions is World, but we make this process recursive, in case we add more in the future.
        while not (composed := tb_regions[tb_regions[subregion_type].isin(tb_regions["name"])]).empty:
            # Break this clause if there is a circular loop.
            iterations += 1
            if iterations > 10:
                raise ValueError("There may be circular dependencies among regions, so subregions cannot be unpacked.")

            for _, row in composed.iterrows():
                # Create a temporary table without the composite subregion.
                tb_without_composite_region = tb_regions[
                    ~((tb_regions["name"] == row["name"]) & (tb_regions[subregion_type] == row[subregion_type]))
                ].reset_index(drop=True)
                # Create a temporary table with only that composite subregion.
                tb_composite_region = tb_regions[tb_regions["name"] == row[subregion_type]].assign(
                    **{"code": row["code"], "name": row["name"]}
                )
                # Replace the original table by the combination of the two temporary tables.
                tb_regions = pr.concat([tb_without_composite_region, tb_composite_region], ignore_index=True)
        tb_regions = tb_regions.set_index("code")

    # Create a column with the list of members in each region
    tb_countries_in_region = (
        tb_regions.rename(columns={"name": "region"})
        .groupby("region", as_index=True, observed=True)
        .agg({subregion_type: lambda x: sorted(set(x))})
    )

    return tb_countries_in_region


def list_members_of_region(
    region: str,
    ds_regions: Dataset,
    ds_income_groups: Dataset | None = None,
    additional_regions: list[str] | None = None,
    excluded_regions: list[str] | None = None,
    additional_members: list[str] | None = None,
    excluded_members: list[str] | None = None,
    custom_members: list[str] | None = None,
    include_historical_regions_in_income_groups: bool = False,
    exclude_historical_countries: bool = False,
    # TODO: Should this be True by default?
    unpack_subregions: bool = False,
) -> list[str]:
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
    exclude_historical_countries : bool
        True to include historical countries.
    unpack_subregions : bool
        True to replace subregions by their countries. For example, for "World", instead of showing "Africa" as a member, it will show all African countries.

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
    if custom_members is None:
        custom_members = []

    # Get the main table from the regions dataset and create a new table that has regions and members.
    tb_countries_in_region = create_table_of_regions_and_subregions(
        ds_regions=ds_regions, subregion_type="members", unpack_subregions=unpack_subregions
    )

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

    # If a list of custom members is given, then use that list instead.
    if any(custom_members):
        countries_set = set(custom_members)

    # Add countries from the list of additional regions.
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

    # Filter historical countries
    if exclude_historical_countries:
        tb_hist = ds_regions["regions"]
        countries_historical = set(tb_hist.loc[tb_hist["is_historical"], "name"])
        countries = [c for c in countries if c not in countries_historical]

    return countries


def detect_overlapping_regions(
    df: TableOrDataFrame,
    index_columns: list[str],
    regions_and_members: dict[Hashable, list[str]],
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
        df.groupby([country_col, year_col], observed=True)
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
    countries_in_data = set(df[country_col].unique().tolist())  # type: ignore
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
    ds_income_groups: Dataset | None = None,
    regions: list[str] | dict[str, Any] | None = None,
    aggregations: dict[str, str] | None = None,
    index_columns: list[str] | None = None,
    num_allowed_nans_per_year: int | None = None,
    frac_allowed_nans_per_year: float | None = None,
    min_num_values_per_year: int | None = None,
    country_col: str = "country",
    year_col: str = "year",
    keep_original_region_with_suffix: str | None = None,
    check_for_region_overlaps: bool = True,
    accepted_overlaps: list[dict[int, set[str]]] | None = None,
    ignore_overlaps_of_zeros: bool = False,
    subregion_type: str = "successors",
    countries_that_must_have_data: dict[str, list[str]] | None = None,
    frac_countries_that_must_have_data: dict[str, float] | None = None,
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
    regions : Optional[Union[list[str], dict[str, Any]]], default: None
        Regions to be added.
        * If it is a list, it must contain region names of default regions or income groups.
          Example: ["Africa", "Europe", "High-income countries"]
        * If it is a dictionary, each key must be the name of a default, or custom region, and the value is another
          dictionary, that can contain any of the following keys:
          * "additional_regions": Additional regions whose members should be included in the region.
          * "excluded_regions": Regions whose members should be excluded from the region.
          * "additional_members": Additional individual members (countries) to include in the region.
          * "excluded_members": Individual members to exclude from the region.
          * "custom_members": Explicit list of countries to include in the region (ignoring the default members).
          Example: {
            "Asia": {},  # No need to define anything, since it is a default region.
            "Asia excluding China": {  # Custom region that must be defined based on other known regions and countries.
                "additional_regions": ["Asia"],
                "excluded_members": ["China"],
                },
            }
        * If None, the default regions will be added (defined as REGIONS in etl.data_helpers.geo).
    aggregations : Optional[dict[str, str]], default: None
        Aggregation to implement for each variable.
        * If a dictionary is given, the keys must be columns of the input data, and the values must be valid operations.
          Only the variables indicated in the dictionary will be affected. All remaining variables will have an
          aggregate value for the new regions of nan.
          Example: {"column_1": "sum", "column_2": "mean", "column_3": lambda x: some_function(x)}
          If there is a "column_4" in the data, for which no aggregation is defined, then the e.g. "Europe" will have
          only nans for "column_4".
        * If None, "sum" will be assumed to all variables.
    index_columns : Optional[list[str]], default: None
        Names of index columns (usually ["country", "year"]). Aggregations will be done on groups defined by these
        columns (excluding the country column). A country and a year column should always be included.
        But more dimensions are also allowed, e.g. index_columns=["country", "year", "type"].
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
    accepted_overlaps : Optional[list[dict[int, set[str]]]], default: None
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
    countries_that_must_have_data : Optional[dict[str, list[str]]], default: None
        * If a dictionary is passed, each key must be a valid region, and the value should be a list of countries that
          must have data for that region. If any of those countries is not informed on a particular variable and year,
          that region will have nan for that particular variable and year.
        * If None, an aggregate is constructed regardless of the countries missing.
    frac_countries_that_must_have_data: dict[str, float] | None, default: None
        * If a dictionary is passed, each key must be a valid region, and the value should be a float between 0 and 1,
          indicating the fraction of countries that must have data for that region. NOTE: Only works if `countries_that_must_have_data` is passed.
        * If None, an aggregate is constructed regardless of the fraction of countries missing. I.e. it assumes that fraction should be 1 (i.e. 100%).


    Returns
    -------
    TableOrDataFrame
        Original table (or dataframe) after adding (or replacing) aggregate data for regions.

    """
    df_with_regions = pd.DataFrame(tb).copy()

    if index_columns is None:
        index_columns = [country_col, year_col]

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
            index_columns=index_columns,
            ignore_overlaps_of_zeros=ignore_overlaps_of_zeros,
        )
        # Example of accepted_overlaps:
        # [{1991: {"Georgia", "USSR"}}, {2000: {"Some region", "Some overlapping region"}}]
        # Check whether all accepted overlaps are found in the data, and that there are no new unknown overlaps.
        accepted_not_found = [overlap for overlap in accepted_overlaps if overlap not in all_overlaps]
        found_not_accepted = [overlap for overlap in all_overlaps if overlap not in accepted_overlaps]
        if len(accepted_not_found):
            log.warning(
                f"Known overlaps not found in the data: {accepted_not_found}. Consider removing them from 'accepted_overlaps'."
            )
        if len(found_not_accepted):
            log.warning(
                f"Unknown overlaps found in the data: {found_not_accepted}. Consider adding them to 'accepted_overlaps'."
            )

    if aggregations is None:
        # Create region aggregates for all columns (with a simple sum) except for index columns.
        aggregations = {column: "sum" for column in df_with_regions.columns if column not in index_columns}

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

    if frac_countries_that_must_have_data:
        error = "Argument frac_countries_that_must_have_data must be a dictionary with regions as keys."
        assert set(frac_countries_that_must_have_data) <= set(regions), error
        # Fill missing regions with an empty list.
        # frac_countries_that_must_have_data = {
        #     region: frac_countries_that_must_have_data.get(region, 1) for region in list(regions)
        # }
    else:
        frac_countries_that_must_have_data = {}  # {region: 1 for region in list(regions)}

    # Add region aggregates.
    for region in regions:
        # Check that the content of the region dictionary is as expected.
        expected_items = {
            "additional_regions",
            "excluded_regions",
            "additional_members",
            "excluded_members",
            "custom_members",
        }
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
            custom_members=regions[region].get("custom_members"),
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
            index_columns=index_columns,
            countries_in_region=members,
            countries_that_must_have_data=countries_that_must_have_data[region],
            frac_countries_that_must_have_data=frac_countries_that_must_have_data.get(region),
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


def fill_date_gaps(tb: Table) -> Table:
    """Ensure dataframe has all dates. We do this by reindexing the dataframe to have all dates for all locations."""
    # Ensure date is of type date
    tb["date"] = pd.to_datetime(tb["date"], format="%Y-%m-%d").astype("datetime64[ns]")

    # Get set of locations
    countries = set(tb["country"])
    # Create index based on all locations and all dates
    complete_dates = pd.date_range(tb["date"].min(), tb["date"].max())

    # Reindex
    tb = tb.set_index(["country", "date"])
    new_index = pd.MultiIndex.from_product([countries, complete_dates], names=["country", "date"])
    tb = tb.reindex(new_index).sort_index().reset_index()

    return tb


def make_table_population_daily(ds_population: Dataset, year_min: int, year_max: int) -> Table:
    """Create table with daily population.

    Uses linear interpolation.
    """
    # Load population table
    population = ds_population.read("population", safe_types=False)
    # Filter only years of interest
    population = population[(population["year"] >= year_min) & (population["year"] <= year_max)]
    # Create date column
    population["date"] = pd.to_datetime(population["year"].astype("string") + "-07-01")
    # Keep relevant columns
    population = population.loc[:, ["date", "country", "population"]]
    # Add missing dates
    population = fill_date_gaps(population)
    # Linearly interpolate NaNs
    population = interpolate_table(population, "country", "date")
    return cast(Table, population)


def add_population_daily(tb: Table, ds_population: Dataset, missing_countries: set | None = None) -> Table:
    """Add `population` column to table.

    Adds population value on a daily basis (extrapolated from yearly data).
    """
    tb["date"] = pd.to_datetime(tb["date"])

    countries_start = set(tb["country"].unique())
    tb_pop = make_table_population_daily(
        ds_population=ds_population, year_min=tb["date"].dt.year.min() - 1, year_max=tb["date"].dt.year.max() + 1
    )
    tb = tb.merge(tb_pop, on=["country", "date"])
    countries_end = set(tb["country"].unique())

    # Check countries that went missing
    if missing_countries is not None:
        countries_missing = countries_start - countries_end
        assert (
            countries_missing == missing_countries
        ), f"Missing countries don't match the expected! {countries_missing}"

    return tb


def countries_to_continent_mapping(
    ds_regions: Dataset,
    ds_income_groups: Dataset | None = None,
    regions: dict[str, Any] | None = None,
    exclude_historical_countries: bool = True,
    include_historical_regions_in_income_groups: bool = False,
) -> dict[str, str]:
    """Get dictionary mapping country names to continents.

    E.g.
        {
            "United States": "North America",
            "Congo": "Africa",
            ...
        }

    Parameters
    ----------
    ds_regions : Dataset
        Regions dataset.
    """
    if regions is None:
        regions = {
            "Africa": {},
            "Asia": {},
            "Europe": {},
            "North America": {},
            "Oceania": {},
            "South America": {},
        }
    countries_to_continent = {}
    for region in regions.keys():
        members = list_members_of_region(
            region=region,
            ds_regions=ds_regions,
            ds_income_groups=ds_income_groups,
            additional_regions=regions[region].get("additional_regions"),
            excluded_regions=regions[region].get("excluded_regions"),
            additional_members=regions[region].get("additional_members"),
            excluded_members=regions[region].get("excluded_members"),
            custom_members=regions[region].get("custom_members"),
            # By default, include historical regions in income groups.
            exclude_historical_countries=exclude_historical_countries,
            include_historical_regions_in_income_groups=include_historical_regions_in_income_groups,
        )
        countries_to_continent |= {m: region for m in members}

    return countries_to_continent


def countries_to_income_mapping(ds_regions: Dataset, ds_income: Dataset):
    """Get dictionary mapping country names to income groups.

    E.g.
        {
            "United States": "High-income countries",
            ...
        }

    Parameters
    ----------
    ds_regions : Dataset
        Regions dataset. Not used in the function, but needed due to strict requirement by `list_members_of_region`.
    ds_income_groups : Optional[Dataset], default: None
        World Bank income groups dataset.
        * If given, aggregates for income groups may be added to the data.
        * If None, no aggregates for income groups will be added.
    """
    regions = [
        "Low-income countries",
        "Lower-middle-income countries",
        "Upper-middle-income countries",
        "High-income countries",
    ]
    countries_to_continent = {}
    for region in regions:
        members = list_members_of_region(
            region=region,
            ds_regions=ds_regions,
            ds_income_groups=ds_income,
            # By default, include historical regions in income groups.
            exclude_historical_countries=True,
        )
        countries_to_continent |= {m: region for m in members}

    return countries_to_continent


def _load_ds_or_raise(ds_name: str, ds_path: Path, auto_load: bool) -> Dataset:
    if auto_load:
        # Auto-load from default location (for standalone usage).
        return Dataset(ds_path)
    else:
        # For ETL work, raise an error if the dataset is not among dependencies of the current step.
        raise ValueError(
            f"{ds_name} dataset could not be loaded. If this is part of an ETL step, add the latest version of the dataset to the list of dependencies."
        )


class Regions:
    """Convenience tool to handle operations related to regions (countries, continents, aggregates, and income groups).

    It can also be used in the context of an ETL data step, e.g. to generate the country name harmonization file, or to apply that harmonization to a table.

    Simply create a regions object:
    > regions = Regions()
    and then:
    - Access the members of a region:
    > regions.get_region("Europe")["members"]
    more generally, paths.regions.get_region("Europe") gives a dictionary with all info of the region given in the regions dataset.

    - Access a list of regions:
    > regions.get_regions(["Africa", "High-income countries"])
    More conveniently
    > regions.get_regions(["Africa", "High-income countries"], only_members=True)
    returns a dictionary {"Africa": ["Algeria", "Angola", ...], "High-income countries": [...], ...}

    The Regions object is instantiated with PathFinder, so, within an ETL step, one can e.g.:
    - Create a countries harmonization file for the current dataset:
    > paths.regions.harmonizer()
    This will start the interactive harmonizer on the interactive window.

    - Apply the country name harmonization to a table, without having to specify the path to the countries file or the excluded countries file.
    > tb = paths.regions.harmonize_names(tb)

    """

    def __init__(
        self,
        ds_regions: Dataset | None = None,
        ds_income_groups: Dataset | None = None,
        ds_population: Dataset | None = None,
        countries_file: Path | str | None = None,
        excluded_countries_file: Path | str | None = None,
        auto_load_datasets: bool = True,
    ):
        # Initialize some hidden attributes to allow for lazy-loading of datasets and tables.
        self._ds_regions = ds_regions
        self._tb_regions = None
        self._ds_income_groups = ds_income_groups
        self._tb_income_groups = None
        self._tb_income_groups_latest = None
        self._ds_population = ds_population
        self._tb_population = None
        self._regions_all = None

        # Other attributes.
        self.countries_file = countries_file
        self.excluded_countries_file = excluded_countries_file
        self._region_cache: dict[str, dict[str, Any]] = {}
        self._informed_countries_cache: dict[tuple, set[str]] = {}
        # If auto_load_datasets is True and no ds_regions is passed, load the latest regions dataset (and idem for ds_income_groups).
        # NOTE: This parameter will be False when Regions is loaded from PathFinder; that way we impose that regions (and/or income_groups) must be among dependencies.
        self.auto_load_datasets = auto_load_datasets

    @property
    def ds_regions(self) -> Dataset:
        """Regions dataset."""
        if self._ds_regions is None:
            self._ds_regions = _load_ds_or_raise(
                ds_name="Regions", ds_path=LATEST_REGIONS_DATASET_PATH, auto_load=self.auto_load_datasets
            )
        return self._ds_regions

    @property
    def ds_income_groups(self) -> Dataset | None:
        """Income groups dataset."""
        if self._ds_income_groups is None:
            self._ds_income_groups = _load_ds_or_raise(
                ds_name="Income groups", ds_path=LATEST_INCOME_DATASET_PATH, auto_load=self.auto_load_datasets
            )
        return self._ds_income_groups

    @property
    def ds_population(self) -> Dataset | None:
        """Population dataset."""
        if self._ds_population is None:
            self._ds_population = _load_ds_or_raise(
                ds_name="Population", ds_path=LATEST_POPULATION_DATASET_PATH, auto_load=self.auto_load_datasets
            )
        return self._ds_population

    @property
    def tb_regions(self) -> Table:
        """Main table from the regions dataset."""
        if self._tb_regions is None:
            self._tb_regions = self.ds_regions.read("regions")
        return self._tb_regions

    @property
    def tb_income_groups(self) -> Table:
        """Table of the income groups dataset that contains income groups classification over the years (not just the latest classification)."""
        if self._tb_income_groups is None:
            self._tb_income_groups = self.ds_income_groups.read("income_groups")  # type: ignore
        return self._tb_income_groups

    @property
    def tb_income_groups_latest(self) -> Table:
        """Table of the income groups dataset that contains the latest income groups classification."""
        if self._tb_income_groups_latest is None:
            self._tb_income_groups_latest = self.ds_income_groups.read("income_groups_latest")  # type: ignore
        return self._tb_income_groups_latest

    @property
    def tb_population(self) -> Table:
        """Main table from the population dataset."""
        if self._tb_population is None:
            self._tb_population = self.ds_population.read("population")  # type: ignore
        return self._tb_population

    def get_region(self, name: str) -> dict:
        """Get region members and other information.

        Parameters
        ----------
        name : str
            Region name (e.g., "Africa", "Europe", "High-income countries").

        Returns
        -------
        dict
            Region members and other information.
        """
        if name not in self._region_cache:
            # Find if given region exists.
            if name in INCOME_GROUPS:
                # Start with a default empty dictionary.
                region_dict = {column: None for column in self.tb_regions.columns}
                # Fill with some information.
                region_dict.update(
                    {  # type: ignore
                        "code": INCOME_GROUPS_ENTITY_CODES[name],
                        "name": name,
                        "region_type": "income_group",
                        "defined_by": "wb",
                        "is_historical": False,
                    }
                )
            else:
                # Try to find region name in the regions table.
                _region = self.tb_regions[self.tb_regions["name"] == name]
                if _region.empty:
                    raise ValueError(f"Region {name} not found")
                # NOTE: If we decide to accept multiple regions with the same name, we could disambiguate by defined_by.
                assert len(_region) == 1, f"Multiple regions found for name {name}"
                region_dict = _region.iloc[0].to_dict()
            # For now, use the existing function to extract members, which has some additional logic.
            region_dict["members"] = list_members_of_region(  # type: ignore
                region=name,
                ds_regions=self.ds_regions,
                # Load income groups only if necessary (and raise an error if not among dependencies).
                ds_income_groups=self.ds_income_groups if name in INCOME_GROUPS else None,
                include_historical_regions_in_income_groups=True,
                unpack_subregions=True,
            )

            self._region_cache[name] = region_dict
        return self._region_cache[name]

    def get_regions(
        self, names: list[str] | None = None, only_members: bool = False
    ) -> dict[str, Any] | dict[str, list[str]]:
        """Get multiple regions.

        Parameters
        ----------
        names : list[str] or None
            List of region names to get. If None, returns all available regions.
        only_members : dict[str, Any]
            True to return only members of regions, e.g. {"Africa": ["Algeria", "Angola", ...], "Asia": ["Afghanistan", ...], ...}.

        Returns
        -------
        dict[str, list[str]]
            Regions as requested format.
        """
        if names is None:
            # Get the full list of names of continents and aggregates (which includes World) and income groups.
            names = self.regions_all
            # If income groups cannot be loaded, remove them from the list.
            if (self._ds_income_groups is None) and not self.auto_load_datasets:
                names = sorted(set(names) - set(INCOME_GROUPS))
        if only_members:
            # Create a dictionary of members of each region.
            regions = {name: self.get_region(name)["members"] for name in names}
        else:
            # Create a dictionary of individual region dictionaries with all information.
            regions = {name: self.get_region(name) for name in names}

        return regions

    @property
    def regions_all(self) -> list[str]:
        # Complete list of names of region that are aggregates (including World) or continents in the regions dataset, and income groups.
        if self._regions_all is None:
            self._regions_all = sorted(
                set(self.tb_regions[self.tb_regions["region_type"].isin(["continent", "aggregate"])]["name"])
                | set(INCOME_GROUPS)
            )
        return self._regions_all

    def harmonizer(self, tb: Table, country_col: str = "country", institution: str | None = None) -> None:
        """Harmonize region names interactively and save mapping to a *.countries.json file (defined by countries_file).

        This tool is meant to be used from a notebook or an interactive window.
        """
        from etl.harmonize import harmonize_ipython

        if self.countries_file is None:
            raise ValueError(
                "A path to a countries file needs to be defined before using harmonizer. Add countries_file argument when initializing Regions."
            )
        else:
            harmonize_ipython(
                tb=tb,
                column=country_col,
                output_file=self.countries_file,
                institution=institution,
            )

    def harmonize_names(
        self,
        tb: Table,
        country_col: str = "country",
        warn_on_missing_countries: bool = True,
        make_missing_countries_nan: bool = False,
        warn_on_unused_countries: bool = True,
        warn_on_unknown_excluded_countries: bool = True,
        show_full_warning: bool = True,
    ) -> Table:
        """Harmonize country names in a table using the countries mapping file."""
        if self.countries_file is None:
            raise ValueError("countries_file must be provided to use harmonize_countries")

        if not Path(self.countries_file).exists():
            raise ValueError(
                "A country mapping must exist before using regions.harmonize_countries. Use regions.harmonizer first."
            )

        return harmonize_countries(
            df=tb,
            countries_file=self.countries_file,
            excluded_countries_file=self.excluded_countries_file,
            country_col=country_col,
            warn_on_missing_countries=warn_on_missing_countries,
            make_missing_countries_nan=make_missing_countries_nan,
            warn_on_unused_countries=warn_on_unused_countries,
            warn_on_unknown_excluded_countries=warn_on_unknown_excluded_countries,
            show_full_warning=show_full_warning,
        )


class RegionAggregator:
    """Manages operations on tables that have, or need to have, region aggregates.

    The aggregator is typically created through the `paths.region_aggregator()` method (using PathFinder), which pre-configures it with the necessary inputs.

    Examples
    --------
    * If you just want to add region aggregates to a table, you can, e.g.:
    > tb = paths.region_aggregator().add_aggregates(tb)

    * You can add per capita indicators in a similar way (regardless of whether you have created region aggregates):
    > tb = paths.region_aggregator().add_per_capita(tb)

    * A more efficient way to achieve these calculations would be:
    > tb_agg = paths.region_aggregator()
    > tb = tb_agg.add_aggregates(tb)
    > tb = tb_agg.add_per_capita(tb)
    This avoids repeating certain calculations twice. However, if your table changes index after creating aggregates (e.g. if you pivot or melt), then you need to define tb_agg again and pass the new index_columns argument.

    Parameters
    ----------
    ds_regions : Dataset
        Regions dataset.
    regions_all : list[str]
        Complete list of all regions (continents, aggregates, income groups, and the World) defined in the regions dataset.
    aggregations : Optional[dict[str, Any]], default: None
        Aggregation to implement for each variable.
        * If a dictionary is given, the keys must be columns of the input data, and the values must be valid operations.
        Only the variables indicated in the dictionary will be affected. All remaining variables will have an
        aggregate value for the new regions of nan.
        Example: {"column_1": "sum", "column_2": "mean", "column_3": lambda x: some_function(x)}
        If there is a "column_4" in the data, for which no aggregation is defined, then the e.g. "Europe" will have
        only nans for "column_4".
        * If None, "sum" will be assumed to all variables.
    regions : Optional[Union[list[str], dict[str, Any]]], default: None
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
    index_columns : Optional[list[str]], default: None
        Names of index columns (usually ["country", "year"]). Aggregations will be done on groups defined by these
        columns (excluding the country column). A country and a year column should always be included.
        But more dimensions are also allowed, e.g. index_columns=["country", "year", "type"].
    ds_income_groups : Optional[Dataset], default: None
        World Bank income groups dataset.
    ds_population : Optional[Dataset], default: None
        Population dataset.
    country_col : str, default 'country'
        Name of the country column in the data.
    year_col : str, default 'year'
        Name of the year column in the data.
    population_col : str, default 'population'
        Name of the population column.

    """

    def __init__(
        self,
        ds_regions: Dataset,
        regions_all: list[str],
        aggregations: dict[str, Any] | None = None,
        regions: list[str] | dict[str, Any] | None = None,
        index_columns: list[str] | None = None,
        ds_income_groups: Dataset | None = None,
        ds_population: Dataset | None = None,
        country_col: str = "country",
        year_col: str = "year",
        population_col: str = "population",
    ):
        self._ds_regions = ds_regions
        self._ds_income_groups = ds_income_groups
        self._ds_population = ds_population
        self.aggregations = aggregations  # type: ignore
        self.country_col = country_col
        self.year_col = year_col
        self.population_col = population_col

        # Coverage table.
        self.tb_coverage = None

        # Fill missing arguments with default values and ensure regions is always a dict.
        if regions is None:
            self.regions: dict[str, Any] = REGIONS
        elif isinstance(regions, list):
            # Assume they are known regions and they have no modifications.
            self.regions = {region: {} for region in regions}
        else:
            # regions is already a dict
            self.regions = regions

        # If "regions" is passed as a list, it can only contain regions that are known to the regions dataset.
        # On the other hand, if it is a dictionary, it can have custom regions.
        if isinstance(regions, list):
            unknown_regions = [region for region in regions if region not in regions_all]
            if unknown_regions:
                raise ValueError(f"Unknown regions in list: {unknown_regions}")

        # Create a list of all possible regions, which includes any possible custom region.
        self.regions_all = sorted(set(regions_all) | set(self.regions))

        # Create a dictionary of all regions and members.
        self.regions_members = self._parse_regions_dict()

        if index_columns is None:
            self.index_columns = [self.country_col, self.year_col]
        else:
            self.index_columns = index_columns

    def _parse_regions_dict(self):
        regions_members = {}
        # Try to load income groups dataset only if required.
        _ds_income_groups = self.ds_income_groups if any(set(self.regions).intersection(set(INCOME_GROUPS))) else None
        for region in self.regions_all:
            if region in self.regions:
                # Check that the content of the region dictionary is as expected.
                expected_items = {
                    "additional_regions",
                    "excluded_regions",
                    "additional_members",
                    "excluded_members",
                    "custom_members",
                }
                unknown_items = set(self.regions[region]) - expected_items
                if len(unknown_items) > 0:
                    log.warning(
                        f"Unknown items in dictionary of regions {region}: {unknown_items}. Expected: {expected_items}."
                    )
                # List members of the region, with possible modifications.
                members = list_members_of_region(
                    region=region,
                    ds_regions=self.ds_regions,
                    ds_income_groups=_ds_income_groups,
                    additional_regions=self.regions[region].get("additional_regions"),
                    excluded_regions=self.regions[region].get("excluded_regions"),
                    additional_members=self.regions[region].get("additional_members"),
                    excluded_members=self.regions[region].get("excluded_members"),
                    custom_members=self.regions[region].get("custom_members"),
                    include_historical_regions_in_income_groups=True,
                    unpack_subregions=True,
                )
                regions_members[region] = members
            else:
                # List default members of the region.
                members = list_members_of_region(
                    region=region,
                    ds_regions=self.ds_regions,
                    ds_income_groups=_ds_income_groups,
                    include_historical_regions_in_income_groups=True,
                    unpack_subregions=True,
                )
                regions_members[region] = members

        return regions_members

    @property
    def ds_regions(self) -> Dataset:
        """Regions dataset."""
        if self._ds_regions is None:
            self._ds_regions = _load_ds_or_raise(
                ds_name="Regions", ds_path=LATEST_REGIONS_DATASET_PATH, auto_load=False
            )
        return self._ds_regions

    @property
    def ds_income_groups(self) -> Dataset | None:
        """Income groups dataset."""
        if self._ds_income_groups is None:
            self._ds_income_groups = _load_ds_or_raise(
                ds_name="Income groups", ds_path=LATEST_INCOME_DATASET_PATH, auto_load=False
            )
        return self._ds_income_groups

    @property
    def ds_population(self) -> Dataset | None:
        """Population dataset."""
        if self._ds_population is None:
            self._ds_population = _load_ds_or_raise(
                ds_name="Population", ds_path=LATEST_POPULATION_DATASET_PATH, auto_load=False
            )
        return self._ds_population

    def _create_coverage_table(self, tb: TableOrDataFrame) -> None:
        """Create data coverage table with the same shape as the original table."""
        # Create a data coverage table, which is 0 if a given cell in the original table was nan, and 1 otherwise.
        self.tb_coverage = Table(tb.notnull())

        # Replace index columns by their original values.
        self.tb_coverage[self.index_columns] = tb[self.index_columns].copy()

    def _ensure_aggregations_are_defined(self, tb: TableOrDataFrame) -> None:
        # If aggregations are not defined, assume all non-index columns have a sum aggregate.
        if self.aggregations is None:
            self.aggregations: dict[str, Any] = {
                column: "sum" for column in tb.columns if column not in self.index_columns
            }

    def _preprocess_table_for_weighted_aggregations(self, tb: Table) -> Table:
        """Add population or other weight columns if needed for weighted aggregations."""
        # Check if any aggregation uses population weighting and population column is missing
        for _, agg_func in self.aggregations.items():
            if isinstance(agg_func, str) and agg_func == f"weighted_by_{self.population_col}":
                if self.population_col not in tb.columns:
                    if self._ds_population is None:
                        raise ValueError(
                            f"Population column '{self.population_col}' not found in table, and no population dataset provided. "
                            f"Add population dataset as a dependency or include population column in your table."
                        )
                    tb = add_population_to_table(
                        tb=tb,
                        ds_population=self.ds_population,  # type: ignore
                        country_col=self.country_col,
                        year_col=self.year_col,
                        population_col=self.population_col,
                        warn_on_missing_countries=False,
                        show_full_warning=True,
                        interpolate_missing_population=False,
                        expected_countries_without_population=None,
                    )
                    break  # Only need to add population once

        return tb

    def _get_needed_columns(self, tb: TableOrDataFrame, columns: list[str]) -> list[str]:
        """Extract the minimal set of columns needed for aggregation performance optimization."""
        weight_columns = []
        for agg_func in self.aggregations.values():
            if isinstance(agg_func, str) and agg_func.startswith("weighted_by_"):
                weight_col = agg_func.replace("weighted_by_", "")
                if weight_col not in weight_columns and weight_col in tb.columns:
                    weight_columns.append(weight_col)

        return self.index_columns + columns + weight_columns

    def inspect_overlaps_with_historical_regions(
        self,
        tb,
        accepted_overlaps: list[dict[int, set[str]]] | None = None,
        ignore_overlaps_of_zeros: bool = False,
        subregion_type: str = "successors",
    ):
        """Check if a historical region has data on the same years as any of its successors, which could lead to double-counting data when creating region aggregates.

        For now, this function raises a warning if an overlap between historical regions and successors is found.


        For example, some datasets include data for the USSR and Russia on the same years. Sometimes this happens just on the year of the dissolution (which makes sense, as they both existed for some time), but sometimes data is just extrapolated (or zero) backwards or forwards in time.

        This function does not check if a region (e.g. Africa) overlaps with any of its members (e.g. Algeria). That is common and should not be cause of warning. Therefore, we don't need to account for custom definitions of regions (which is about region memberships, rather than successors).

        TODO: Consider adding the option to remove those overlaps.
        """
        if accepted_overlaps is None:
            accepted_overlaps = []

        # Create a dictionary of historical regions and its successors.
        df_regions_and_members = create_table_of_regions_and_subregions(
            ds_regions=self.ds_regions, subregion_type=subregion_type
        )
        regions_and_members = df_regions_and_members[subregion_type].to_dict()

        # Assume incoming table has a dummy index (the whole function may not work otherwise).
        # Example of region_and_members:
        # {"Czechoslovakia": ["Czechia", "Slovakia"]}
        all_overlaps = detect_overlapping_regions(
            df=tb,
            regions_and_members=regions_and_members,
            country_col=self.country_col,
            year_col=self.year_col,
            index_columns=self.index_columns,
            ignore_overlaps_of_zeros=ignore_overlaps_of_zeros,
        )
        # Example of accepted_overlaps:
        # [{1991: {"Georgia", "USSR"}}, {2000: {"Some region", "Some overlapping region"}}]
        # Check whether all accepted overlaps are found in the data, and that there are no new unknown overlaps.
        accepted_not_found = [overlap for overlap in accepted_overlaps if overlap not in all_overlaps]
        found_not_accepted = [overlap for overlap in all_overlaps if overlap not in accepted_overlaps]
        if len(accepted_not_found):
            log.warning(
                f"Known overlaps not found in the data: {accepted_not_found}. Consider removing them from 'accepted_overlaps'."
            )
        if len(found_not_accepted):
            log.warning(
                f"Unknown overlaps found in the data: {found_not_accepted}. Consider adding them to 'accepted_overlaps'."
            )

    def _create_table_of_only_region_aggregates(
        self,
        tb: TableOrDataFrame,
        regions: list[str],
        aggregations: dict[str, Any],
        num_allowed_nans_per_year: int | None = None,
        frac_allowed_nans_per_year: float | None = None,
        min_num_values_per_year: int | None = None,
    ):
        # Check if we have any weighted aggregations that need special handling
        weighted_columns = {
            col: agg_func
            for col, agg_func in aggregations.items()
            if isinstance(agg_func, str) and agg_func.startswith("weighted_by_")
        }
        non_weighted_aggregations = {
            col: agg_func
            for col, agg_func in aggregations.items()
            if not (isinstance(agg_func, str) and agg_func.startswith("weighted_by_"))
        }

        # Create region aggregates.
        dfs_with_regions = []
        for region in regions:
            # Select data for countries in the region.
            df_region_data = tb[tb[self.country_col].isin(self.regions_members[region])]

            if df_region_data.empty:
                # Create empty result with proper columns
                empty_result = pd.DataFrame(columns=tb.columns)
                if not empty_result.empty:
                    dfs_with_regions.append(empty_result)
                continue

            # Handle non-weighted aggregations using the existing groupby_agg function
            if non_weighted_aggregations:
                df_region = groupby_agg(
                    df=df_region_data,
                    groupby_columns=[column for column in self.index_columns if column != self.country_col],
                    aggregations=non_weighted_aggregations,
                    num_allowed_nans=num_allowed_nans_per_year,
                    frac_allowed_nans=frac_allowed_nans_per_year,
                    min_num_values=min_num_values_per_year,
                ).reset_index()
            else:
                # Create a basic dataframe with index columns if we only have weighted aggregations
                groupby_columns = [column for column in self.index_columns if column != self.country_col]
                if groupby_columns:
                    df_region = df_region_data[groupby_columns].drop_duplicates().reset_index(drop=True)
                else:
                    df_region = pd.DataFrame([{}])  # Single row for aggregation

            # Handle weighted aggregations separately
            if weighted_columns:
                df_region = self._add_weighted_aggregations(
                    df_region=df_region,
                    df_region_data=df_region_data,
                    weighted_columns=weighted_columns,
                    num_allowed_nans_per_year=num_allowed_nans_per_year,
                    frac_allowed_nans_per_year=frac_allowed_nans_per_year,
                    min_num_values_per_year=min_num_values_per_year,
                )

            # Add a column for region name.
            df_region[self.country_col] = region

            dfs_with_regions.append(df_region)

        # Concatenate aggregates of all regions.
        # If no region aggregates were created, return an empty Table with the same columns as the original.
        dfs_with_regions_non_empty = [df for df in dfs_with_regions if not df.empty]
        df_with_regions = (
            pd.concat(dfs_with_regions_non_empty, ignore_index=True)
            if dfs_with_regions_non_empty
            else pd.DataFrame(columns=tb.columns)
        )

        return df_with_regions

    def _add_weighted_aggregations(
        self,
        df_region: pd.DataFrame,
        df_region_data: pd.DataFrame,
        weighted_columns: dict[str, Any],
        num_allowed_nans_per_year: int | None = None,
        frac_allowed_nans_per_year: float | None = None,
        min_num_values_per_year: int | None = None,
    ) -> pd.DataFrame:
        """Add weighted aggregation columns to the region dataframe."""
        groupby_columns = [column for column in self.index_columns if column != self.country_col]

        if not groupby_columns:
            # No grouping columns, aggregate all data
            for col, agg_func in weighted_columns.items():
                weight_column = agg_func.replace("weighted_by_", "")  # Extract weight column from string
                weighted_mean = self._calculate_weighted_mean(
                    df_region_data,
                    col,
                    weight_column,
                    num_allowed_nans_per_year,
                    frac_allowed_nans_per_year,
                    min_num_values_per_year,
                )
                df_region[col] = weighted_mean
        else:
            # Group by the specified columns and calculate weighted means
            for col, agg_func in weighted_columns.items():
                weight_column = agg_func.replace("weighted_by_", "")  # Extract weight column from string

                # Only select the columns we need for grouping and calculation (performance optimization)
                # Check if all needed columns exist first
                needed_columns = groupby_columns + [col, weight_column]
                missing_columns = [c for c in needed_columns if c not in df_region_data.columns]
                if missing_columns:
                    if weight_column in missing_columns:
                        raise ValueError(f"Weight column '{weight_column}' not found in data")
                    elif col in missing_columns:
                        raise ValueError(f"Value column '{col}' not found in data")
                    else:
                        raise ValueError(f"Required columns not found in data: {missing_columns}")

                df_subset = df_region_data[needed_columns]

                # Calculate weighted mean for each group
                grouped_results = []
                for group_values, group_data in df_subset.groupby(groupby_columns):
                    weighted_mean = self._calculate_weighted_mean(
                        group_data,
                        col,
                        weight_column,
                        num_allowed_nans_per_year,
                        frac_allowed_nans_per_year,
                        min_num_values_per_year,
                    )

                    # Create a row with the group values and weighted mean
                    if isinstance(group_values, tuple):
                        result_row = dict(zip(groupby_columns, group_values))
                    else:
                        result_row = {groupby_columns[0]: group_values}
                    result_row[col] = weighted_mean
                    grouped_results.append(result_row)

                # Merge the results back to df_region
                if grouped_results:
                    weighted_results = pd.DataFrame(grouped_results)
                    if df_region.empty:
                        df_region = weighted_results
                    else:
                        # Convert both to regular pandas DataFrames for merging to avoid catalog/pandas conflicts
                        df_region_pd = pd.DataFrame(df_region) if not isinstance(df_region, pd.DataFrame) else df_region
                        # Use pandas merge directly to avoid catalog merge complications
                        merged = pd.merge(df_region_pd, weighted_results, on=groupby_columns, how="left")
                        df_region = merged

        return df_region

    def _calculate_weighted_mean(
        self,
        data: pd.DataFrame,
        value_col: str,
        weight_col: str,
        num_allowed_nans: int | None = None,
        frac_allowed_nans: float | None = None,
        min_num_values: int | None = None,
    ) -> float:
        """Calculate weighted mean for the given data, handling NaN values according to the specified rules."""
        if value_col not in data.columns:
            raise ValueError(f"Value column '{value_col}' not found in data")
        if weight_col not in data.columns:
            raise ValueError(f"Weight column '{weight_col}' not found in data")

        values = data[value_col]
        weights = data[weight_col]

        # Remove rows where either value or weight is NaN or weight is zero
        mask = ~(pd.isna(values) | pd.isna(weights) | (weights == 0))
        valid_values = values[mask]
        valid_weights = weights[mask]

        # Apply NaN handling rules similar to groupby_agg
        total_count = len(values)
        valid_count = len(valid_values)
        nan_count = total_count - valid_count

        # Check num_allowed_nans condition
        if num_allowed_nans is not None and nan_count > num_allowed_nans:
            return np.nan

        # Check frac_allowed_nans condition
        if frac_allowed_nans is not None and total_count > 0:
            nan_fraction = nan_count / total_count
            if nan_fraction > frac_allowed_nans:
                return np.nan

        # Check min_num_values condition
        if min_num_values is not None and valid_count < min_num_values:
            # Exception: if all values in the group are valid, accept it even if count < min_num_values
            if nan_count > 0:
                return np.nan

        # Calculate weighted mean if we have valid data
        if len(valid_values) == 0:
            return np.nan

        return np.average(valid_values, weights=valid_weights)

    def _impose_countries_that_must_have_data(self, df_only_regions, columns, countries_that_must_have_data):
        # List all index columns except the country column.
        other_index_columns = [column for column in self.index_columns if column != self.country_col]
        for column in columns:
            for region, countries in countries_that_must_have_data.items():
                if df_only_regions[df_only_regions[self.country_col] == region].empty:
                    continue
                # Create a temporary dataframe of groupings where all required countries are informed.
                df_covered = (
                    self.tb_coverage[self.tb_coverage[column]][self.index_columns]  # type: ignore
                    .groupby(other_index_columns, as_index=False)
                    .agg({self.country_col: lambda x: set(countries) <= set(x)})
                )
                # Detect indexes where not all required countries are informed.
                _make_nan = df_covered[~df_covered[self.country_col]].assign(**{self.country_col: region})
                # Make those rows nan in the dataframe with only regions.
                merged = df_only_regions.merge(_make_nan, on=self.index_columns, how="left", indicator=True)
                df_only_regions.loc[merged["_merge"] == "both", column] = np.nan

    def add_aggregates(
        self,
        tb: Table,
        num_allowed_nans_per_year: int | None = None,
        frac_allowed_nans_per_year: float | None = None,
        min_num_values_per_year: int | None = None,
        check_for_region_overlaps: bool = True,
        accepted_overlaps: list[dict[int, set[str]]] | None = None,
        ignore_overlaps_of_zeros: bool = False,
        subregion_type: str = "successors",
        countries_that_must_have_data: dict[str, list[str]] | None = None,
    ) -> Table:
        """Add region aggregates to a table (or dataframe).

        This should be the default function to use when adding data for regions to a table (or dataframe).
        This function respects the metadata of the incoming data.

        NOTE: We used to have the argument keep_original_region_with_suffix, but it was barely used. If you want to keep the original regions with a suffix, or exclude them, you can still do that before creating aggregates, when harmonizing country names.

        Parameters
        ----------
        tb : TableOrDataFrame
            Original data, which may or may not contain data for regions.
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
        check_for_region_overlaps : bool, default: True
            * If True, a warning is raised if a historical region has data on the same year as any of its successors.
            * If False, any possible overlap is ignored.
        accepted_overlaps : Optional[list[dict[int, set[str]]]], default: None
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
        countries_that_must_have_data : Optional[dict[str, list[str]]], default: None
            * If a dictionary is passed, each key must be a valid region, and the value should be a list of countries that
            must have data for that region. If any of those countries is not informed on a particular variable and year,
            that region will have nan for that particular variable and year.
            * If None, an aggregate is constructed regardless of the countries missing.

        Returns
        -------
        Table:
            Original table (or dataframe) after adding (or replacing) aggregate data for regions.

        """
        # Ensure aggregations are well defined.
        self._ensure_aggregations_are_defined(tb=tb)

        # Preprocess table to add weight columns if needed for weighted aggregations
        tb = self._preprocess_table_for_weighted_aggregations(tb)

        # Define the list of (non-index) columns for which aggregates will be created.
        columns = list(self.aggregations)

        # Extract only the columns we actually need to improve performance.
        needed_columns = self._get_needed_columns(tb, columns)
        tb_fast = tb[needed_columns]
        other_columns = [col for col in tb.columns if col not in needed_columns]

        # Run everything on the fast subset instead of full DataFrame
        if check_for_region_overlaps:
            self.inspect_overlaps_with_historical_regions(
                tb=tb_fast,
                accepted_overlaps=accepted_overlaps,
                ignore_overlaps_of_zeros=ignore_overlaps_of_zeros,
                subregion_type=subregion_type,
            )

        # Create region aggregates on fast subset
        df_only_regions = self._create_table_of_only_region_aggregates(
            tb=tb_fast,
            regions=list(self.regions),
            aggregations=self.aggregations,
            num_allowed_nans_per_year=num_allowed_nans_per_year,
            frac_allowed_nans_per_year=frac_allowed_nans_per_year,
            min_num_values_per_year=min_num_values_per_year,
        )

        if countries_that_must_have_data is not None:
            if self.tb_coverage is None:
                self._create_coverage_table(tb=tb_fast)
            self._impose_countries_that_must_have_data(
                df_only_regions=df_only_regions,
                columns=columns,
                countries_that_must_have_data=countries_that_must_have_data,
            )

        # Create a mask that selects rows of regions in the original data, if any.
        _select_regions = tb[self.country_col].isin(list(self.regions))

        # If there were regions in other columns (not used for aggregates) include them in the subtable of only regions.
        if any(other_columns) and any(_select_regions):
            df_only_regions = df_only_regions.merge(
                tb[_select_regions][self.index_columns + other_columns], how="outer", on=self.index_columns
            )

        # Create a table of all other rows that are not regions.
        df_no_regions = tb[~_select_regions]

        # Combine the table with only regions and the table with no regions.
        df_with_regions = pd.concat([df_only_regions, df_no_regions], ignore_index=True)  # type: ignore

        # Final sort and column ordering
        df_with_regions = df_with_regions.sort_values(self.index_columns).reset_index(drop=True)[tb.columns]

        # Convert country to categorical if the original was
        if tb[self.country_col].dtype.name == "category":
            df_with_regions = df_with_regions.astype({self.country_col: "category"})

        # If the original object was a Table, copy metadata
        if isinstance(tb, Table):
            return Table(df_with_regions).copy_metadata(tb)
        else:
            return df_with_regions  # type: ignore

    def add_per_capita(
        self,
        tb: Table,
        only_informed_countries_in_regions: bool = False,
        columns: list[str] | None = None,
        prefix: str = "",
        suffix: str = "_per_capita",
        suffix_informed_population: str = "_region_population",
        drop_population: bool | None = None,
        warn_on_missing_countries: bool = True,
        show_full_warning: bool = True,
        interpolate_missing_population: bool = False,
        expected_countries_without_population: list[str] | None = None,
    ) -> Table:
        """Add per-capita indicators.

        Parameters
        ----------
        tb : Table
            Table where per-capita indicators will be created.
        only_informed_countries_in_regions : bool
            True to construct per-capita indicators of regions taking into account the data coverage of that region each year. For example, if "Africa" is among countries, the population of Africa will be calculated, for each indicator, based on the African countries informed each year for that indicator. Otherwise, if only_informed_countries_in_regions is False, the indicator will be divided by the entire population of Africa each year, regardless of data coverage.
        columns : list[str] or None
            Columns to convert to per-capita. If None, all columns except country and year will be used.
        prefix : str
            Prefix to prepend to the original column names to create the name of the new per-capita column.
        suffix : str
            Suffix to append to the original column names to create the name of the new per-capita column.
        suffix_informed_population : str
            Suffix to use for auxiliary columns of informed population. Only relevant if only_informed_countries_in_regions is True.
        drop_population : bool or None
            True to drop the population column after creating per capita indicators. If None, population column will be dropped only if it wasn't already given in the original table.
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
        Table
            Table with additional per-capita columns.
        """
        tb_result = tb.copy()

        self._ensure_aggregations_are_defined(tb=tb)

        # Check if population was originally given in the data.
        was_population_in_table = self.population_col in tb_result.columns

        if columns is None:
            columns = [
                column for column in tb_result.columns if column not in self.index_columns + [self.population_col]
            ]

        # Add population to table, if not there yet.
        if not was_population_in_table:
            tb_result = add_population_to_table(
                tb=tb,
                ds_population=self.ds_population,  # type: ignore
                country_col=self.country_col,
                year_col=self.year_col,
                population_col=self.population_col,
                warn_on_missing_countries=warn_on_missing_countries,
                show_full_warning=show_full_warning,
                interpolate_missing_population=interpolate_missing_population,
                expected_countries_without_population=expected_countries_without_population,
            )

        if only_informed_countries_in_regions:
            # Find the list of all possible regions included in the table (including continents and other aggregates like "World").
            regions = sorted(set(tb["country"]) & set(self.regions_all))
            # Ensure a table of data coverage exists, otherwise calculate it.
            if self.tb_coverage is None:
                self._create_coverage_table(tb=tb_result)
            # Create an auxiliary table of informed population. For a given column, each row contains the population of the corresponding region on the corresponding year, or a zero, if that column-row was originally nan.
            tb_population_informed = self.tb_coverage[self.index_columns + columns].copy()  # type: ignore
            tb_population_informed[columns] = tb_population_informed[columns].multiply(
                tb_result[self.population_col], axis=0
            )
            # NOTE: Here we do this calculation for all per capita columns. We could do this just for the intersection of per capita columns and aggregation columns. However, the current implementation is more informative.
            tb_population_informed = self._create_table_of_only_region_aggregates(
                tb=tb_population_informed,
                regions=regions,
                aggregations={column: "sum" for column in columns},
            )
            # For each per capita column, add an auxiliary columns of informed population.
            # These columns will only have data for rows corresponding to region aggregates (and be nan for individual countries).
            tb_result = tb_result.merge(
                tb_population_informed, on=self.index_columns, how="left", suffixes=("", suffix_informed_population)
            )

        for col in columns:
            new_col_name = f"{prefix}{col}{suffix}"
            if only_informed_countries_in_regions:
                # Divide the original column by the population of informed countries in the region each year.
                # NOTE: The auxiliary columns of informed population have only data for regions. For individual countries, we fill missing values with the full population.
                tb_result[new_col_name] = tb_result[col] / tb_result[f"{col}{suffix_informed_population}"].fillna(
                    tb_result[self.population_col]
                )
            else:
                # Divide the original column by the population of the region, regardless of the coverage.
                tb_result[new_col_name] = tb_result[col] / tb_result[self.population_col]

        if drop_population is None:
            # If parameter drop_population is not specified (namely, if it is None), then drop population column only if it wasn't already in the original table.
            drop_population = not was_population_in_table

        if drop_population:
            tb_result = tb_result.drop(columns=self.population_col, errors="raise")
            if only_informed_countries_in_regions:
                # Drop columns of informed population.
                tb_result = tb_result.drop(
                    columns=[column for column in tb_result.columns if column.endswith(suffix_informed_population)],
                    errors="raise",
                )

        return tb_result
