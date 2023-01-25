import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union, cast

import numpy as np
import pandas as pd
from owid import catalog
from structlog import get_logger

from etl.data_helpers import geo
from etl.paths import DATA_DIR

CURRENT_DIR = Path(__file__).parent

log = get_logger()

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
    "World": {},
    # Income groups.
    "Low-income countries": {},
    "Upper-middle-income countries": {},
    "Lower-middle-income countries": {},
    "High-income countries": {},
}

# When creating region aggregates, decide how to distribute historical regions.
# The following decisions are based on the current location of the countries that succeeded the region, and their income
# group. Continent and income group assigned corresponds to the continent and income group of the majority of the
# population in the member countries.
HISTORIC_TO_CURRENT_REGION: Dict[str, Dict[str, Union[str, List[str]]]] = {
    "Czechoslovakia": {
        "continent": "Europe",
        "income_group": "High-income countries",
        "regions_included": [
            # Europe - High-income countries.
            "Czechia",
            "Slovakia",
        ],
    },
    "East Germany": {
        "continent": "Europe",
        "income_group": "",
        "regions_included": [
            # Europe - High-income countries.
            "Germany",
        ],
    },
    "West Germany": {
        "continent": "Europe",
        "income_group": "",
        "regions_included": [
            # Europe - High-income countries.
            "Germany",
        ],
    },
    "Netherlands Antilles": {
        "continent": "North America",
        "income_group": "High-income countries",
        "regions_included": [
            # North America - High-income countries.
            "Aruba",
            "Curacao",
            "Sint Maarten (Dutch part)",
            "Bonaire Sint Eustatius and Saba",
        ],
    },
    "Serbia and Montenegro": {
        "continent": "Europe",
        "income_group": "Upper-middle-income countries",
        "regions_included": [
            # Europe - Upper-middle-income countries.
            "Serbia",
            "Montenegro",
        ],
    },
    "North Yemen": {
        "continent": "Asia",
        "income_group": "Low-income countries",
        "regions_included": [
            # Asia - Low-income countries.
            "Yemen",
        ],
    },
    "South Yemen": {
        "continent": "Asia",
        "income_group": "Low-income countries",
        "regions_included": [
            # Asia - Low-income countries.
            "Yemen",
        ],
    },
    "USSR": {
        "continent": "Europe",
        "income_group": "Upper-middle-income countries",
        "regions_included": [
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
        "regions_included": [
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

# Historical countries whose population can be built by adding up the population of their successor countries.
# Those historical countries not listed here will have no population data.
BUILD_POPULATION_FOR_HISTORICAL_COUNTRIES = [
    # The following regions split into smaller ones, and can be estimated by the population of the successors.
    "Czechoslovakia",
    "Netherlands Antilles",
    "Serbia and Montenegro",
    "USSR",
    "Yugoslavia",
    # The following countries cannot be replaced by the successor countries.
    # 'East Germany',
    # 'West Germany',
    # 'North Yemen',
    # 'South Yemen',
]

# Historical countries for which we don't have population, and can't be built from successor countries.
EXPECTED_COUNTRIES_WITHOUT_POPULATION = list(
    set(HISTORIC_TO_CURRENT_REGION) - set(BUILD_POPULATION_FOR_HISTORICAL_COUNTRIES)
)

# Overlaps found between historical regions and successor countries, that we accept in the data.
# We accept them either because they happened close to the transition, or to avoid needing to introduce new
# countries for which we do not have data (like the Russian Empire).
ACCEPTED_OVERLAPS = {
    1902: {"USSR", "Azerbaijan"},
    1990: {"Tajikistan", "USSR"},
    1991: {"Georgia", "USSR"},
}


def get_countries_in_region(
    region: str, region_modifications: Optional[Dict[str, Dict[str, List[str]]]] = None
) -> List[str]:
    """Get countries in a region, both for known regions (e.g. "Africa") and custom ones (e.g. "Europe (excl. EU-27)").

    Parameters
    ----------
    region : str
        Region name (e.g. "Africa", or "Europe (excl. EU-27)").
    region_modifications : dict or None
        If None (or an empty dictionary), the region should be in OWID's countries-regions dataset.
        If not None, it should be a dictionary with any (or all) of the following keys:
        - "regions_included": List of regions whose countries will be included.
        - "regions_excluded": List of regions whose countries will be excluded.
        - "countries_included": List of additional individual countries to be included.
        - "countries_excluded": List of additional individual countries to be excluded.
        NOTE: All regions and countries defined in this dictionary should be in OWID's countries-regions dataset.

    Returns
    -------
    countries : list
        List of countries in the specified region.

    """
    if region_modifications is None:
        region_modifications = {}

    # Check that the fields in the regions_modifications dictionary are well defined.
    expected_fields = ["regions_included", "regions_excluded", "countries_included", "countries_excluded"]
    assert all([field in expected_fields for field in region_modifications])

    # Get lists of regions whose countries will be included and excluded.
    regions_included = region_modifications.get("regions_included", [region])
    regions_excluded = region_modifications.get("regions_excluded", [])
    # Get lists of additional individual countries to include and exclude.
    countries_included = region_modifications.get("countries_included", [])
    countries_excluded = region_modifications.get("countries_excluded", [])

    # List countries from the list of regions included.
    countries_set = set(
        sum([geo.list_countries_in_region(region_included) for region_included in regions_included], [])
    )

    # Remove all countries from the list of regions excluded.
    countries_set -= set(
        sum([geo.list_countries_in_region(region_excluded) for region_excluded in regions_excluded], [])
    )

    # Add the list of individual countries to be included.
    countries_set |= set(countries_included)

    # Remove the list of individual countries to be excluded.
    countries_set -= set(countries_excluded)

    # Convert set of countries into a sorted list.
    countries = sorted(countries_set)

    return countries


def load_population(regions: Optional[Dict[Any, Any]] = None) -> pd.DataFrame:
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

    # Consider additional regions (e.g. historical regions).
    if regions is None:
        regions = {}
    missing_countries = [country for country in regions if country not in countries_with_population]
    for country in missing_countries:
        members = regions[country]["regions_included"]
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


def add_population(
    df: pd.DataFrame,
    country_col: str = "country",
    year_col: str = "year",
    population_col: str = "population",
    interpolate_missing_population: bool = False,
    warn_on_missing_countries: bool = True,
    show_full_warning: bool = True,
    regions: Optional[Dict[Any, Any]] = None,
    expected_countries_without_population: List[str] = [],
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

    # Load population dataset.
    population = load_population(regions=regions).rename(
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


def detect_overlapping_regions(
    df, index_columns, region_and_members, country_col="country", year_col="year", ignore_zeros=True
):
    """Detect years on which the data for two regions overlap, e.g. a historical region and one of its successors.

    Parameters
    ----------
    df : _type_
        Data (with a dummy index).
    index_columns : _type_
        Names of index columns.
    region_and_members : _type_
        Regions to check for overlaps. Each region must have a dictionary "regions_included", listing the subregions
        contained. If the region is historical, "regions_included" would be the list of successor countries.
    country_col : str, optional
        Name of country column (usually "country").
    year_col : str, optional
        Name of year column (usually "year").
    ignore_zeros : bool, optional
        True to ignore overlaps of zeros.

    Returns
    -------
    all_overlaps : dict
        All overlaps found.

    """
    # Sum over all columns to get the total sum of each column for each country-year.
    df_total = (
        df.groupby([country_col, year_col])
        .agg({column: "sum" for column in df.columns if column not in index_columns})
        .reset_index()
    )
    # Create a list of values that will be ignored in overlaps (usually zero or nothing).
    if ignore_zeros:
        overlapping_values_to_ignore = [0]
    else:
        overlapping_values_to_ignore = []
    # List all variables in data (ignoring index columns).
    variables = [column for column in df.columns if column not in index_columns]
    # List all country names found in data.
    countries_in_data = df[country_col].unique().tolist()
    # List all regions found in data.
    regions = [country for country in list(region_and_members) if country in countries_in_data]
    # Initialize a dictionary that will store all overlaps found.
    all_overlaps = {}
    for region in regions:
        # List members of current region.
        members = [member for member in region_and_members[region]["regions_included"] if member in countries_in_data]
        for member in members:
            # Select data for current region.
            region_values = (
                df_total[df_total[country_col] == region]
                .replace(overlapping_values_to_ignore, np.nan)
                .dropna(subset=variables, how="all")
            )
            # Select data for current member.
            member_values = (
                df_total[df_total[country_col] == member]
                .replace(overlapping_values_to_ignore, np.nan)
                .dropna(subset=variables, how="all")
            )
            # Concatenate both selections of data, and select duplicated rows.
            combined = pd.concat([region_values, member_values])
            overlaps = combined[combined.duplicated(subset=[year_col], keep=False)]  # type: ignore
            if len(overlaps) > 0:
                # Add the overlap found to the dictionary of all overlaps.
                all_overlaps.update({year: set(overlaps[country_col]) for year in overlaps[year_col].unique()})

    # Sort overlaps conveniently.
    all_overlaps = {year: all_overlaps[year] for year in sorted(list(all_overlaps))}

    return all_overlaps


def add_region_aggregates(
    data: pd.DataFrame,
    index_columns: List[str],
    country_column: str = "country",
    aggregates: Optional[Dict[str, str]] = None,
) -> pd.DataFrame:
    """Add region aggregates for all regions (which may include continents and income groups).

    Parameters
    ----------
    data : pd.DataFrame
        Data.
    index_columns : list
        Name of index columns.
    country_column : str
        Name of country column.
    year_column : str
        Name of year column.
    aggregates : dict or None
        Dictionary of type of aggregation to use for each variable. If None, variables will be aggregated by summing.

    Returns
    -------
    data : pd.DataFrame
        Data after adding aggregate regions.

    """
    data = data.copy()

    all_overlaps = detect_overlapping_regions(
        df=data, region_and_members=HISTORIC_TO_CURRENT_REGION, index_columns=index_columns
    )

    # Check whether all accepted overlaps are found in the data, and that there are no new unknown overlaps.
    error = "Either the list of accepted overlaps is not found in the data, or there are new unknown overlaps."
    assert ACCEPTED_OVERLAPS == all_overlaps, error

    if aggregates is None:
        # If aggregations are not specified, assume all variables are to be aggregated, by summing.
        aggregates = {column: "sum" for column in data.columns if column not in index_columns}

    for region in REGIONS:
        # List of countries in region.
        countries_in_region = get_countries_in_region(region=region, region_modifications=REGIONS[region])
        # Select rows of data for member countries.
        data_region = data[data[country_column].isin(countries_in_region)]

        # Add region aggregates.
        region_df = (
            data_region.groupby([column for column in index_columns if column != country_column])
            .sum(numeric_only=True)
            .reset_index()
            .assign(**{country_column: region})
        )
        data = pd.concat([data, region_df], ignore_index=True)  # type: ignore

    return data


def get_last_day_of_month(year: int, month: int):
    """Get the number of days in a specific month of a specific year.

    Parameters
    ----------
    year : int
        Year.
    month : int
        Month.

    Returns
    -------
    last_day
        Number of days in month.

    """
    if month == 12:
        last_day = 31
    else:
        last_day = (datetime.datetime.strptime(f"{year:04}-{month + 1:02}", "%Y-%m") + datetime.timedelta(days=-1)).day

    return last_day


def correct_data_points(df: pd.DataFrame, corrections: List[Tuple[Dict[Any, Any], Dict[Any, Any]]]) -> pd.DataFrame:
    """Make individual corrections to data points in a dataframe.

    Parameters
    ----------
    df : pd.DataFrame
        Data to be corrected.
    corrections : List[Tuple[Dict[Any, Any], Dict[Any, Any]]]
        Corrections.

    Returns
    -------
    corrected_df : pd.DataFrame
        Corrected data.

    """
    corrected_df = df.copy()

    for correction in corrections:
        wrong_row, corrected_row = correction

        # Select the row in the dataframe where the wrong data point is.
        # The 'fillna(False)' is added because otherwise rows that do not fulfil the selection will create ambiguity.
        selection = corrected_df.loc[(corrected_df[list(wrong_row)] == pd.Series(wrong_row)).fillna(False).all(axis=1)]
        # Sanity check.
        error = "Either raw data has been corrected, or dictionary selecting wrong row is ambiguous."
        assert len(selection) == 1, error

        # Replace wrong fields by the corrected ones.
        # Note: Changes to categorical fields will not work.
        corrected_df.loc[selection.index, list(corrected_row)] = list(corrected_row.values())

    return corrected_df
