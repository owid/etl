from pathlib import Path
from typing import Dict, List, Optional, Union, cast

import numpy as np
import pandas as pd
from owid import catalog
from structlog import get_logger

from etl.data_helpers import geo
from etl.paths import DATA_DIR

log = get_logger()

CURRENT_DIR = Path(__file__).parent
VERSION = CURRENT_DIR.name

# Aggregate regions to add, following OWID definitions.
REGIONS_TO_ADD = {
    "North America": {
        "country_code": "OWID_NAM",
    },
    "South America": {
        "country_code": "OWID_SAM",
    },
    "Europe": {
        "country_code": "OWID_EUR",
    },
    # The EU27 is already included in the original BP data, with the same definition as OWID.
    # "European Union (27)": {
    #     "country_code": "OWID_EU27",
    # },
    "Africa": {
        "country_code": "OWID_AFR",
    },
    "Asia": {
        "country_code": "OWID_ASI",
    },
    "Oceania": {
        "country_code": "OWID_OCE",
    },
    "Low-income countries": {
        "country_code": "OWID_LIC",
    },
    "Upper-middle-income countries": {
        "country_code": "OWID_UMC",
    },
    "Lower-middle-income countries": {
        "country_code": "OWID_LMC",
    },
    "High-income countries": {
        "country_code": "OWID_HIC",
    },
}

# We need to include the 'Other * (BP)' regions, otherwise continents have incomplete data.
# For example, when constructing the aggregate for Africa, we need to include 'Other Africa (BP)'.
# Otherwise we would be underestimating the region's total contribution.
ADDITIONAL_COUNTRIES_IN_REGIONS = {
    "Africa": [
        # Additional African regions in BP's data (e.g. 'Other Western Africa (BP)') seem to be included in
        # 'Other Africa (BP)', therefore we ignore them when creating aggregates.
        "Other Africa (BP)",
    ],
    "Asia": [
        # Adding 'Other Asia Pacific (BP)' may include areas of Oceania in Asia.
        # However, it seems that this region is usually significantly smaller than Asia.
        # So, we are possibly overestimating Asia, but not by a significant amount.
        "Other Asia Pacific (BP)",
        # Similarly, adding 'Other CIS (BP)' in Asia may include areas of Europe in Asia (e.g. Moldova).
        # However, since most countries in 'Other CIS (BP)' are Asian, adding it is more accurate than not adding it.
        "Other CIS (BP)",
        # Countries defined by BP in 'Middle East' are fully included in OWID's definition of Asia.
        "Other Middle East (BP)",
    ],
    "Europe": [
        "Other Europe (BP)",
    ],
    "North America": [
        "Other Caribbean (BP)",
        "Other North America (BP)",
    ],
    "South America": [
        "Other South America (BP)",
    ],
    # Given that 'Other Asia and Pacific (BP)' is often similar or even larger than Oceania, we avoid including it in
    # Oceania (and include it in Asia, see comment above).
    # This means that we may be underestimating Oceania by a significant amount, but BP does not provide unambiguous
    # data to avoid this.
    "Oceania": [],
}

# When creating region aggregates, decide how to distribute historical regions.
# The following decisions are based on the current location of the countries that succeeded the region, and their income
# group. Continent and income group assigned corresponds to the continent and income group of the majority of the
# population in the member countries.
HISTORIC_TO_CURRENT_REGION: Dict[str, Dict[str, Union[str, List[str]]]] = {
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
}


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


def detect_overlapping_data_for_regions_and_members(
    df: pd.DataFrame,
    index_columns: List[str],
    regions_and_members: Dict[str, Dict[str, Union[str, List[str]]]],
    known_overlaps: Optional[List[Dict[str, Union[str, List[int]]]]],
    ignore_zeros: bool = True,
) -> None:
    """Raise a warning if there is data for a particular region and for a country that is a member of that region.

    For example, if there is data for USSR and Russia on the same years, a warning will be raised.

    Parameters
    ----------
    df : pd.DataFrame
        Data.
    index_columns : list
        Names of columns that should be index of the data.
    regions_and_members : dict
        Regions and members (where each key corresponds to a region, and each region is a dictionary of various keys,
        one of which is 'members', which is a list of member countries).
    known_overlaps : list or None
        Instances of known overlaps in the data. If this function raises a warning, new instances should be added to the
        list.
    ignore_zeros : bool
        True to consider zeros in the data as missing values. Doing this, if a region has overlapping data with a member
        country, but one of their data points is zero, it will not be considered an overlap.

    """
    if known_overlaps is not None:
        df = df.copy()

        if ignore_zeros:
            # Replace zeros by nans, so that zeros are ignored when looking for overlapping data.
            overlapping_values_to_ignore = [0]
        else:
            overlapping_values_to_ignore = []

        regions = list(regions_and_members)
        for region in regions:
            # Create a dataframe with only data for the region, and remove columns that only have nans.
            # Optionally, replace zeros by nans, to also remove columns that only have zeros or nans.
            region_df = (
                df[df["country"] == region].replace(overlapping_values_to_ignore, np.nan).dropna(axis=1, how="all")
            )
            members = regions_and_members[region]["members"]
            for member in members:
                # Create a dataframe for this particular member country.
                member_df = (
                    df[df["country"] == member].replace(overlapping_values_to_ignore, np.nan).dropna(axis=1, how="all")
                )
                # Find common columns with (non-nan) data between region and member country.
                variables = [
                    column
                    for column in (set(region_df.columns) & set(member_df.columns))
                    if column not in index_columns
                ]
                for variable in variables:
                    # Concatenate region and member country's data for this variable.
                    combined = (
                        pd.concat(
                            [
                                region_df[["year", variable]],
                                member_df[["year", variable]],
                            ],
                            ignore_index=True,
                        )
                        .dropna()
                        .reset_index(drop=True)
                    )
                    # Find years where region and member country overlap.
                    overlapping = combined[combined.duplicated(subset="year")]
                    if not overlapping.empty:
                        overlapping_years = sorted(set(overlapping["year"]))
                        new_overlap = {
                            "region": region,
                            "member": member,
                            "years": overlapping_years,
                            "variable": variable,
                        }
                        # Check if the overlap found is already in the list of known overlaps.
                        # If this overlap is not known, raise a warning.
                        # Omit the field "entity_to_make_nan" when checking if this overlap is known.
                        _known_overlaps = [
                            {key for key in overlap if key != "entity_to_make_nan"} for overlap in known_overlaps
                        ]
                        if new_overlap not in _known_overlaps:  # type: ignore
                            log.warning(
                                f"Data for '{region}' overlaps with '{member}' on '{variable}' "
                                f"and years: {overlapping_years}"
                            )


def remove_overlapping_data_for_regions_and_members(
    df: pd.DataFrame,
    known_overlaps: Optional[List[Dict[str, Union[str, List[int]]]]],
    country_col: str = "country",
    year_col: str = "year",
    ignore_zeros: bool = True,
) -> pd.DataFrame:
    """Check if list of known overlaps between region (e.g. a historical region like the USSR) and a member country (or
    a successor country, like Russia) do overlap, and remove them from the data.

    Parameters
    ----------
    df : pd.DataFrame
        Data.
    known_overlaps : list or None
        List of known overlaps between region and member country.
    country_col : str
        Name of country column.
    year_col : str
        Name of year column.
    ignore_zeros : bool
        True to ignore columns of zeros when checking if known overlaps are indeed overlaps.

    Returns
    -------
    df : pd.DataFrame
        Data after removing known overlapping rows between a region and a member country.

    """
    if known_overlaps is not None:
        df = df.copy()

        if ignore_zeros:
            overlapping_values_to_ignore = [0]
        else:
            overlapping_values_to_ignore = []

        for i, overlap in enumerate(known_overlaps):
            if set([overlap["region"], overlap["member"]]) <= set(df["country"]):
                # Check that the known overlap is indeed found in the data.
                duplicated_rows = (
                    df[(df[country_col].isin([overlap["region"], overlap["member"]]))][
                        [country_col, year_col, overlap["variable"]]
                    ]
                    .replace(overlapping_values_to_ignore, np.nan)
                    .dropna(subset=overlap["variable"])
                )
                duplicated_rows = duplicated_rows[duplicated_rows.duplicated(subset="year", keep=False)]
                overlapping_years = sorted(set(duplicated_rows["year"]))
                if overlapping_years != overlap["years"]:
                    log.warning(f"Given overlap number {i} is not found in the data; redefine this list.")
                # Make nan data points for either the region or the member (which is specified by "entity to make nan").
                indexes_to_make_nan = duplicated_rows[
                    duplicated_rows["country"] == overlap[overlap["entity_to_make_nan"]]  # type: ignore
                ].index.tolist()
                df.loc[indexes_to_make_nan, overlap["variable"]] = np.nan

    return df


def load_countries_in_regions() -> Dict[str, List[str]]:
    """Create a dictionary of regions (continents and income groups) and their member countries.

    Regions to include are defined above, in REGIONS_TO_ADD.
    Additional countries are added to regions following the definitions in ADDITIONAL_COUNTRIES_IN_REGIONS.

    Returns
    -------
    countries_in_regions : dict
        Dictionary of regions, where the value is a list of member countries in the region.

    """
    # Load income groups.
    income_groups = load_income_groups()

    countries_in_regions = {}
    for region in list(REGIONS_TO_ADD):
        # Add default OWID list of countries in region (which includes historical regions).
        countries_in_regions[region] = geo.list_countries_in_region(region=region, income_groups=income_groups)

    # Include additional countries in the region (if any given).
    for region in ADDITIONAL_COUNTRIES_IN_REGIONS:
        countries_in_regions[region] = countries_in_regions[region] + ADDITIONAL_COUNTRIES_IN_REGIONS[region]

    return countries_in_regions


def add_region_aggregates(
    data: pd.DataFrame,
    regions: List[str],
    index_columns: List[str],
    country_column: str = "country",
    year_column: str = "year",
    aggregates: Optional[Dict[str, str]] = None,
    known_overlaps: Optional[List[Dict[str, Union[str, List[int]]]]] = None,
    region_codes: Optional[List[str]] = None,
    country_code_column: str = "country_code",
) -> pd.DataFrame:
    """Add region aggregates for all regions (which may include continents and income groups).

    Parameters
    ----------
    data : pd.DataFrame
        Data.
    regions : list
        Regions to include.
    index_columns : list
        Name of index columns.
    country_column : str
        Name of country column.
    year_column : str
        Name of year column.
    aggregates : dict or None
        Dictionary of type of aggregation to use for each variable. If None, variables will be aggregated by summing.
    known_overlaps : list or None
        List of known overlaps between regions and their member countries.
    region_codes : list or None
        List of country codes for each new region. It must have the same number of elements, and in the same order, as
        the 'regions' argument.
    country_code_column : str
        Name of country codes column (only relevant of region_codes is not None).

    Returns
    -------
    data : pd.DataFrame
        Data after adding aggregate regions.

    """
    data = data.copy()

    if aggregates is None:
        # If aggregations are not specified, assume all variables are to be aggregated, by summing.
        aggregates = {column: "sum" for column in data.columns if column not in index_columns}
    # Get the list of regions to create, and their member countries.
    countries_in_regions = load_countries_in_regions()
    for region in regions:
        # List of countries in region.
        countries_in_region = countries_in_regions[region]
        # Select rows of data for member countries.
        data_region = data[data[country_column].isin(countries_in_region)]
        # Remove any known overlaps between regions (e.g. USSR, which is a historical region) in current region (e.g.
        # Europe) and their member countries (or successor countries, like Russia).
        # If any overlap in known_overlaps is not found, a warning will be raised.
        data_region = remove_overlapping_data_for_regions_and_members(df=data_region, known_overlaps=known_overlaps)

        # Check that there are no other overlaps in the data (after having removed the known ones).
        detect_overlapping_data_for_regions_and_members(
            df=data_region,
            regions_and_members=HISTORIC_TO_CURRENT_REGION,
            index_columns=index_columns,
            known_overlaps=known_overlaps,
        )

        # Add region aggregates.
        data_region = geo.add_region_aggregates(
            df=data_region,
            region=region,
            country_col=country_column,
            year_col=year_column,
            aggregations=aggregates,
            countries_in_region=countries_in_region,
            countries_that_must_have_data=[],
            # Here we allow aggregating even when there are few countries informed (which seems to agree with BP's
            # criterion for aggregates).
            # However, if absolutely all countries have nan, we want the aggregate to be nan, not zero.
            frac_allowed_nans_per_year=0.999,
            num_allowed_nans_per_year=None,
        )
        data = pd.concat(
            [data, data_region[data_region[country_column] == region]],
            ignore_index=True,
        ).reset_index(drop=True)

    if region_codes is not None:
        # Add region codes to regions.
        if data[country_code_column].dtype == "category":
            data[country_code_column] = data[country_code_column].cat.add_categories(region_codes)
        for i, region in enumerate(regions):
            data.loc[data[country_column] == region, country_code_column] = region_codes[i]

    return data
