import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Dataset, Table, Variable
from structlog import get_logger

from etl.data_helpers import geo

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
    1911: {"Kazakhstan", "USSR"},
    1991: {"Georgia", "USSR"},
}


def detect_overlapping_regions(
    tb, index_columns, region_and_members, country_col="country", year_col="year", ignore_zeros=True
):
    """Detect years on which the data for two regions overlap, e.g. a historical region and one of its successors.

    Parameters
    ----------
    tb : _type_
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
    tb_total = (
        tb.groupby([country_col, year_col])
        .agg({column: "sum" for column in tb.columns if column not in index_columns})
        .reset_index()
    )
    # Create a list of values that will be ignored in overlaps (usually zero or nothing).
    if ignore_zeros:
        overlapping_values_to_ignore = [0]
    else:
        overlapping_values_to_ignore = []
    # List all variables in data (ignoring index columns).
    variables = [column for column in tb.columns if column not in index_columns]
    # List all country names found in data.
    countries_in_data = tb[country_col].unique().tolist()
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
            overlaps = combined[combined.duplicated(subset=[year_col], keep=False)]  # type: ignore
            if len(overlaps) > 0:
                # Add the overlap found to the dictionary of all overlaps.
                all_overlaps.update({year: set(overlaps[country_col]) for year in overlaps[year_col].unique()})

    # Sort overlaps conveniently.
    all_overlaps = {year: all_overlaps[year] for year in sorted(list(all_overlaps))}

    return all_overlaps


def add_region_aggregates(
    data: Table,
    regions_to_add: Dict[Any, Any],
    index_columns: List[str],
    ds_regions: Dataset,
    ds_income_groups: Dataset,
    country_column: str = "country",
    aggregates: Optional[Dict[str, str]] = None,
) -> Table:
    """Add region aggregates for all regions (which may include continents and income groups).

    Parameters
    ----------
    data : Table
        Data.
    regions_to_add: list
        Regions to add.
    index_columns : list
        Name of index columns.
    ds_regions : Dataset
        Regions dataset.
    ds_income_groups : Dataset
        Income groups dataset.
    country_column : str
        Name of country column.
    year_column : str
        Name of year column.
    aggregates : dict or None
        Dictionary of type of aggregation to use for each variable. If None, variables will be aggregated by summing.

    Returns
    -------
    data : Table
        Data after adding aggregate regions.

    """
    data = data.copy()

    all_overlaps = detect_overlapping_regions(
        tb=data, region_and_members=HISTORIC_TO_CURRENT_REGION, index_columns=index_columns
    )

    # Check whether all accepted overlaps are found in the data, and that there are no new unknown overlaps.
    error = "Either the list of accepted overlaps is not found in the data, or there are new unknown overlaps."
    assert ACCEPTED_OVERLAPS == all_overlaps, error

    if aggregates is None:
        # If aggregations are not specified, assume all variables are to be aggregated, by summing.
        aggregates = {column: "sum" for column in data.columns if column not in index_columns}

    for region in regions_to_add:
        # List of countries in region.
        countries_in_region = geo.list_members_of_region(
            region=region, ds_regions=ds_regions, ds_income_groups=ds_income_groups
        )
        # Select rows of data for member countries.
        data_region = data[data[country_column].isin(countries_in_region)]

        # Add region aggregates.
        region_df = (
            data_region.groupby([column for column in index_columns if column != country_column])
            .agg(aggregates)
            .reset_index()
            .assign(**{country_column: region})
        )
        data = pr.concat([data, region_df], ignore_index=True)

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


def correct_data_points(tb: Table, corrections: List[Tuple[Dict[Any, Any], Dict[Any, Any]]]) -> Table:
    """Make individual corrections to data points in a table.

    Parameters
    ----------
    tb : Table
        Data to be corrected.
    corrections : List[Tuple[Dict[Any, Any], Dict[Any, Any]]]
        Corrections.

    Returns
    -------
    tb_corrected : Table
        Corrected data.

    """
    tb_corrected = tb.copy()

    for correction in corrections:
        wrong_row, corrected_row = correction

        # Select the row in the table where the wrong data point is.
        # The 'fillna(False)' is added because otherwise rows that do not fulfil the selection will create ambiguity.
        selection = tb_corrected.loc[(tb_corrected[list(wrong_row)] == Variable(wrong_row)).fillna(False).all(axis=1)]
        # Sanity check.
        error = "Either raw data has been corrected, or dictionary selecting wrong row is ambiguous."
        assert len(selection) == 1, error

        # Replace wrong fields by the corrected ones.
        # Note: Changes to categorical fields will not work.
        tb_corrected.loc[selection.index, list(corrected_row)] = list(corrected_row.values())

    return tb_corrected
