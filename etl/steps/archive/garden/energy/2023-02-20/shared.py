from typing import List

import pandas as pd
from owid import catalog

from etl.data_helpers import geo


def gather_sources_from_tables(
    tables: List[catalog.Table],
) -> List[catalog.meta.Source]:
    """Gather unique sources from the metadata.dataset of each table in a list of tables.

    Note: To check if a source is already listed, only the name of the source is considered (not the description or any
    other field in the source).

    Parameters
    ----------
    tables : list
        List of tables with metadata.

    Returns
    -------
    known_sources : list
        List of unique sources from all tables.

    """
    # Initialise list that will gather all unique metadata sources from the tables.
    known_sources: List[catalog.meta.Source] = []
    for table in tables:
        # Get list of sources of the dataset of current table.
        table_sources = table.metadata.dataset.sources
        # Go source by source of current table, and check if its name is not already in the list of known_sources.
        for source in table_sources:
            # Check if this source's name is different to all known_sources.
            if all([source.name != known_source.name for known_source in known_sources]):
                # Add the new source to the list.
                known_sources.append(source)

    return known_sources


def add_population(
    df: pd.DataFrame,
    population: pd.DataFrame,
    country_col: str = "country",
    year_col: str = "year",
    population_col: str = "population",
    interpolate_missing_population: bool = False,
    warn_on_missing_countries: bool = True,
    show_full_warning: bool = True,
    expected_countries_without_population: List[str] = [],
) -> pd.DataFrame:
    """Add a column of OWID population to the countries in the data, including population of historical regions.

    Parameters
    ----------
    df : pd.DataFrame
        Data without a column for population (after harmonizing elements, items and country names).
    population : pd.DataFrame
        Population data.
    country_col : str
        Name of country column in data.
    year_col : str
        Name of year column in data.
    population_col : str
        Name for new population column in data.
    interpolate_missing_population : bool
        True to linearly interpolate population on years that are presented in df, but for which we do not have
        population data; otherwise False to keep missing population data as nans.
        For example, if interpolate_missing_population is True and df has data for all years between 1900 and 1910,
        but population is only given for 1900 and 1910, population will be linearly interpolated between those years.
    warn_on_missing_countries : bool
        True to warn if population is not found for any of the countries in the data.
    show_full_warning : bool
        True to show affected countries if the previous warning is raised.
    regions : dict
        Definitions of regions whose population also needs to be included.
    expected_countries_without_population : list
        Countries that are expected to not have population (that should be ignored if warnings are activated).

    Returns
    -------
    df_with_population : pd.DataFrame
        Data after adding a column for population for all countries in the data.

    """

    # Prepare population dataset.
    population = population.reset_index().rename(
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

        error = "Countries without population data differs from list of expected countries without population data."
        assert set(population[population[population_col].isnull()].reset_index()[country_col]) == set(
            expected_countries_without_population
        ), error

    # Add population to original dataframe.
    df_with_population = pd.merge(df, population, on=[country_col, year_col], how="left")

    return df_with_population
