"""Garden step for EIA total energy consumption.

"""

from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Initialize log.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Conversion factor from terajoules to terawatt-hours.
TJ_TO_TWH = 1 / 3600

# Columns to use from meadow table, and how to rename them.
COLUMNS = {"country": "country", "year": "year", "values": "energy_consumption"}

# Aggregate regions to add, following OWID definitions.
REGIONS_TO_ADD = [
    "North America",
    "South America",
    "Europe",
    "European Union (27)",
    "Africa",
    "Asia",
    "Oceania",
    "Low-income countries",
    "Upper-middle-income countries",
    "Lower-middle-income countries",
    "High-income countries",
]

# Additional countries to include in region aggregates.
ADDITIONAL_COUNTRIES_IN_REGIONS: Dict[str, List[str]] = {}

# When creating region aggregates, decide how to distribute historical regions.
# TODO: Once the regions dataset is refactored, the following information could be extracted directly from ds_regions.
# The following decisions are based on the current location of the countries that succeeded the region, and their income
# group.
HISTORIC_TO_CURRENT_REGION: Dict[str, Dict[str, Union[str, List[str]]]] = {
    "Czechoslovakia": {
        "members": [
            # Europe - High-income countries.
            "Czechia",
            "Slovakia",
        ],
    },
    "Netherlands Antilles": {
        "members": [
            # North America - High-income countries.
            "Aruba",
            "Curacao",
            "Sint Maarten (Dutch part)",
        ],
    },
    "Serbia and Montenegro": {
        "members": [
            # Europe - Upper-middle-income countries.
            "Serbia",
            "Montenegro",
        ],
    },
    "USSR": {
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

# List of known overlaps between regions and member countries (or successor countries).
OVERLAPPING_DATA_TO_REMOVE_IN_AGGREGATES = [
    {
        "region": "Netherlands Antilles",
        "member": "Aruba",
        "entity_to_make_nan": "region",
        "years": [
            1986,
            1987,
            1988,
            1989,
            1990,
            1991,
            1992,
            1993,
            1994,
            1995,
            1996,
            1997,
            1998,
            1999,
            2000,
            2001,
            2002,
            2003,
            2004,
            2005,
            2006,
            2007,
            2008,
            2009,
            2010,
            2011,
            2012,
            2013,
            2014,
            2015,
            2016,
            2017,
            2018,
            2019,
            2020,
            2021,
        ],
        "variable": "energy_consumption",
    }
]


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


def add_region_aggregates(
    data: Table,
    regions: List[str],
    index_columns: List[str],
    ds_regions: Dataset,
    ds_income_groups: Dataset,
    country_column: str = "country",
    year_column: str = "year",
    aggregates: Optional[Dict[str, str]] = None,
    known_overlaps: Optional[List[Dict[str, Union[str, List[int]]]]] = None,
    keep_original_region_with_suffix: Optional[str] = None,
) -> Table:
    """Add region aggregates for all regions (which may include continents and income groups).

    Parameters
    ----------
    data : pd.DataFrame
        Data.
    regions : list
        Regions to include.
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
    known_overlaps : list or None
        List of known overlaps between regions and their member countries.
    region_codes : list or None
        List of country codes for each new region. It must have the same number of elements, and in the same order, as
        the 'regions' argument.
    country_code_column : str
        Name of country codes column (only relevant of region_codes is not None).
    keep_original_region_with_suffix : str or None
        If None, original data for region will be replaced by aggregate data constructed by this function. If not None,
        original data for region will be kept, with the same name, but having suffix keep_original_region_with_suffix
        added to its name.

    Returns
    -------
    data : pd.DataFrame
        Data after adding aggregate regions.

    """
    data_with_regions = data.copy()

    if aggregates is None:
        # If aggregations are not specified, assume all variables are to be aggregated, by summing.
        aggregates = {column: "sum" for column in data_with_regions.columns if column not in index_columns}

    for region in regions:
        # List of countries in region.
        countries_in_region = geo.list_members_of_region(
            region=region, ds_regions=ds_regions, ds_income_groups=ds_income_groups
        )
        # Select rows of data for member countries.
        data_for_region = data_with_regions[data_with_regions[country_column].isin(countries_in_region)]
        # Remove any known overlaps between regions (e.g. USSR, which is a historical region) in current region (e.g.
        # Europe) and their member countries (or successor countries, like Russia).
        # If any overlap in known_overlaps is not found, a warning will be raised.
        data_for_region = remove_overlapping_data_for_regions_and_members(
            df=data_for_region, known_overlaps=known_overlaps
        )

        # Check that there are no other overlaps in the data (after having removed the known ones).
        detect_overlapping_data_for_regions_and_members(
            df=data_for_region,
            regions_and_members=HISTORIC_TO_CURRENT_REGION,
            index_columns=index_columns,
            known_overlaps=known_overlaps,
        )

        # Add region aggregates.
        data_for_region = geo.add_region_aggregates(
            df=data_for_region,
            region=region,
            country_col=country_column,
            year_col=year_column,
            aggregations=aggregates,
            countries_in_region=countries_in_region,
            countries_that_must_have_data=[],
            # Here we allow aggregating even when there are few countries informed.
            # However, if absolutely all countries have nan, we want the aggregate to be nan, not zero.
            frac_allowed_nans_per_year=0.999,
            num_allowed_nans_per_year=None,
            keep_original_region_with_suffix=keep_original_region_with_suffix,
        )
        data_with_regions = pd.concat(
            [
                data_with_regions[data_with_regions[country_column] != region],
                data_for_region[data_for_region[country_column] == region],
            ],
            ignore_index=True,
        ).reset_index(drop=True)

    # Copy metadata of original table to new table with regions.
    data_with_regions = data_with_regions.copy_metadata(from_table=data)

    return data_with_regions


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load EIA dataset and read its main table.
    ds_meadow: Dataset = paths.load_dependency("energy_consumption")
    tb_meadow = ds_meadow["energy_consumption"].reset_index()

    # Load regions dataset.
    ds_regions: Dataset = paths.load_dependency("regions")

    # Load income groups dataset.
    ds_income_groups: Dataset = paths.load_dependency("income_groups")

    #
    # Process data.
    #
    # Select and rename columns conveniently.
    tb = tb_meadow[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Convert terajoules to terawatt-hours.
    tb["energy_consumption"] *= TJ_TO_TWH

    # Create aggregate regions.
    tb = add_region_aggregates(
        data=tb,
        regions=REGIONS_TO_ADD,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        index_columns=["country", "year"],
        known_overlaps=OVERLAPPING_DATA_TO_REMOVE_IN_AGGREGATES,  # type: ignore
    )

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset (with the same metadata as the meadow version).
    ds_garden = create_dataset(
        dest_dir=dest_dir, tables=[tb], default_metadata=ds_meadow.metadata, check_variables_metadata=True
    )
    ds_garden.save()
