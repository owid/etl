"""Process the BP Statistical Review of World Energy 2022.

For the moment, this dataset is downloaded and processed by 
https://github.com/owid/importers/tree/master/bp_statreview

However, in this additional step we add region aggregates.

"""

from copy import deepcopy
from typing import Dict, List, Optional, Union, cast

import numpy as np
import pandas as pd
from owid.datautils import geo

from owid import catalog
from shared import CURRENT_DIR, log

NAMESPACE = "bp"
DATASET_SHORT_NAME = "bp_statistical_review"
# Path to metadata file.
METADATA_FILE_PATH = CURRENT_DIR / "bp_statistical_review.meta.yml"
# Original BP's Statistical Review dataset name in the owid catalog (without the institution and year).
BP_CATALOG_NAME = "statistical_review_of_world_energy"
BP_NAMESPACE_IN_CATALOG = "bp_statreview"
BP_VERSION = 2022
# Previous BP's Statistical Review dataset.
BP_CATALOG_NAME_OLD = "statistical_review_of_world_energy"
BP_NAMESPACE_IN_CATALOG_OLD = "bp_statreview"
BP_VERSION_OLD = 2021

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

# +
# TODO: Consider ignoring regions when constructing aggregates, like New Caledonia
# -

# When creating region aggregates, decide how to distribute historical regions.
# The following decisions are based on the current location of the countries that succeeded the region, and their income
# group. Continent and income group assigned corresponds to the continent and income group of the majority of the
# population in the member countries.
HISTORIC_TO_CURRENT_REGION = {
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

OVERLAPPING_DATA_TO_REMOVE_IN_AGGREGATES = [
    {
        "region": "USSR",
        "member": "Russia",
        "entity_to_make_nan": "region",
        "years": [1991, 1992, 1993, 1994, 1995, 1996],
        "variable": "Gas - Proved reserves",
    }
]

# TODO: Confirm these assignments.
ADDITIONAL_COUNTRIES_IN_REGIONS = {
    "Africa": [
        "Other Africa (BP)",
        "Other Eastern Africa (BP)",
        "Other Middle Africa (BP)",
        "Other Northern Africa (BP)",
        "Other Southern Africa (BP)",
        "Other Western Africa (BP)",
    ],
    "Asia": [
        "Other Asia Pacific (BP)",
        "Other CIS (BP)",
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
        "Other South and Central America (BP)",
    ],
}

# Variables that can be summed when constructing region aggregates.
AGGREGATES_BY_SUM = [
    'Biofuels Consumption - Kboed - Biodiesel',
    'Biofuels Consumption - Kboed - Total',
    'Biofuels Consumption - PJ - Biodiesel',
    'Biofuels Consumption - PJ - Total',
    'Biofuels Consumption - TWh - Biodiesel',
    'Biofuels Consumption - TWh - Total',
    'Biofuels Production - Kboed - Biodiesel',
    'Biofuels Production - Kboed - Total',
    'Biofuels Production - PJ - Biodiesel',
    'Biofuels Production - PJ - Total',
    'Biofuels Production - TWh - Biodiesel',
    'Biofuels Production - TWh - Total',
    'Carbon Dioxide Emissions',
    'Coal - Reserves - Anthracite and bituminous',
    'Coal - Reserves - Sub-bituminous and lignite',
    'Coal - Reserves - Total',
    'Coal Consumption - EJ',
    'Coal Consumption - TWh',
    'Coal Production - EJ',
    'Coal Production - TWh',
    'Coal Production - Tonnes',
    'Cobalt Production-Reserves',
    'Elec Gen from Coal',
    'Elec Gen from Gas',
    'Elec Gen from Oil',
    'Electricity Generation',
    'Gas - Proved reserves',
    'Gas Consumption - Bcf',
    'Gas Consumption - Bcm',
    'Gas Consumption - EJ',
    'Gas Consumption - TWh',
    'Gas Production - Bcf',
    'Gas Production - Bcm',
    'Gas Production - EJ',
    'Gas Production - TWh',
    'Geo Biomass Other - EJ',
    'Geo Biomass Other - TWh',
    'Graphite Production-Reserves',
    'Hydro Consumption - EJ',
    'Hydro Consumption - TWh',
    'Hydro Generation - TWh',
    'Lithium Production-Reserves',
    'Nuclear Consumption - EJ',
    'Nuclear Consumption - TWh',
    'Nuclear Generation - TWh',
    'Oil - Proved reserves',
    'Oil - Refinery throughput',
    'Oil - Refining capacity',
    'Oil Consumption - Barrels',
    'Oil Consumption - EJ',
    'Oil Consumption - TWh',
    'Oil Consumption - Tonnes',
    'Oil Production - Barrels',
    'Oil Production - Crude Conds',
    'Oil Production - NGLs',
    'Oil Production - TWh',
    'Oil Production - Tonnes',
    'Primary Energy Consumption - EJ',
    'Primary Energy Consumption - TWh',
    'Rare Earth Production-Reserves',
    'Renewables Consumption - EJ',
    'Renewables Consumption - TWh',
    'Renewables Power - EJ',
    'Renewables power - TWh',
    'Solar Capacity',
    'Solar Consumption - EJ',
    'Solar Consumption - TWh',
    'Solar Generation - TWh',
    'Total Liquids - Consumption',
    'Wind Capacity',
    'Wind Consumption - EJ',
    'Wind Consumption - TWh',
    'Wind Generation - TWh',
    # 'Biofuels Consumption - TWh - Biodiesel (zero filled)',
    # 'Biofuels Consumption - TWh - Total (zero filled)',
    # 'Coal - Prices',
    # 'Coal Consumption - TWh (zero filled)',
    # 'Gas - Prices',
    # 'Gas Consumption - TWh (zero filled)',
    # 'Geo Biomass Other - TWh (zero filled)',
    # 'Hydro Consumption - TWh (zero filled)',
    # 'Nuclear Consumption - TWh (zero filled)',
    # 'Oil - Crude prices since 1861 (2021 $)',
    # 'Oil - Crude prices since 1861 (current $)',
    # 'Oil - Spot crude prices',
    # 'Oil Consumption - TWh (zero filled)',
    # 'Primary Energy - Cons capita',
    # 'Solar Consumption - TWh (zero filled)',
    # 'Wind Consumption - TWh (zero filled)',
    ]


def load_income_groups() -> pd.DataFrame:
    """Load dataset of income groups and add historical regions to it.

    Returns
    -------
    income_groups : pd.DataFrame
        Income groups data.

    """
    income_groups = (
        catalog.find(
            table="wb_income_group",
            dataset="wb_income",
            namespace="wb",
            channels=["garden"],
        )
        .load()
        .reset_index()
    )
    # Add historical regions to income groups.
    for historic_region in HISTORIC_TO_CURRENT_REGION:
        historic_region_income_group = HISTORIC_TO_CURRENT_REGION[historic_region][
            "income_group"
        ]
        if historic_region not in income_groups["country"]:
            historic_region_df = pd.DataFrame(
                {
                    "country": [historic_region],
                    "income_group": [historic_region_income_group],
                }
            )
            income_groups = pd.concat(
                [income_groups, historic_region_df], ignore_index=True
            )

    return cast(pd.DataFrame, income_groups)


def detect_overlapping_data_for_regions_and_members(
    df: pd.DataFrame, list_of_countries: List[str], index_columns: List[str],
        known_overlaps: Optional[List[Dict[str, Union[str, List[int]]]]], ignore_zeros: bool = True) -> None:
    if known_overlaps is not None:
        df = df.copy()

        # TODO: Add documentation.
        if ignore_zeros:
            overlapping_values_to_ignore = [0]
        else:
            overlapping_values_to_ignore = []

        regions = sorted(set(list_of_countries) & set(HISTORIC_TO_CURRENT_REGION))
        for region in regions:
            region_df = df[df["country"] == region].replace(overlapping_values_to_ignore, np.nan).\
                dropna(axis=1, how="all")
            members = HISTORIC_TO_CURRENT_REGION[region]["members"]
            for member in members:
                member_df = df[df["country"] == member].\
                    replace(overlapping_values_to_ignore, np.nan).dropna(axis=1, how="all")
                variables = [column for column in (set(region_df.columns) & set(member_df.columns))
                             if column not in index_columns]
                for variable in variables:
                    combined = pd.concat([region_df[["year", variable]], member_df[["year", variable]]],
                                         ignore_index=True).dropna().reset_index(drop=True)
                    overlapping = combined[combined.duplicated(subset="year")]
                    if not overlapping.empty:
                        overlapping_years = sorted(set(overlapping["year"]))
                        new_overlap = {
                            "region": region,
                            "member": member,
                            "years": overlapping_years,
                            "variable": variable
                        }
                        # If this overlap is not known, raise a warning.
                        # Omit the field "entity_to_make_nan" when checking if this overlap is known.
                        _known_overlaps = {{key for key in overlap if key != "entity_to_make_nan"}
                                           for overlap in known_overlaps}
                        if new_overlap not in _known_overlaps:
                            log.warning(f"Data for '{region}' overlaps with '{member}' on '{variable}' "
                                        f"and years: {overlapping_years}")


def remove_overlapping_data_for_regions_and_members(
    df, known_overlaps: Optional[List[Dict[str, Union[str, List[int]]]]], country_col: str = "country",
        year_col: str = "year", ignore_zeros: bool = True) -> pd.DataFrame:
    if known_overlaps is not None:
        df = df.copy()
        
        if ignore_zeros:
            overlapping_values_to_ignore = [0]
        else:
            overlapping_values_to_ignore = []

        for i, overlap in enumerate(known_overlaps):
            if set([overlap["region"], overlap["member"]]) <= set(df["country"]):
                # Check that the overlap exists indeed.
                duplicated_rows = df[(df[country_col].isin([overlap["region"], overlap["member"]]))][
                    [country_col, year_col, overlap["variable"]]].replace(overlapping_values_to_ignore, np.nan).\
                    dropna(subset=overlap["variable"])
                duplicated_rows = duplicated_rows[duplicated_rows.duplicated(subset="year", keep=False)]
                overlapping_years = sorted(set(duplicated_rows["year"]))
                if overlapping_years != overlap["years"]:
                    log.warning(f"Given overlap number {i} is not found in the data; redefine this list.")
                # Make nan data points for either the region or the member (which is specified by "entity to make nan").
                indexes_to_make_nan = duplicated_rows[duplicated_rows["country"] ==
                                                      overlap[overlap["entity_to_make_nan"]]].index.tolist()
                df.loc[indexes_to_make_nan, overlap["variable"]] = np.nan

    return df


def load_countries_in_regions():
    income_groups=load_income_groups()

    countries_in_region = {}
    for region in list(REGIONS_TO_ADD):
        countries_in_region[region] = geo.list_countries_in_region(
                    region=region, income_groups=income_groups
        )

    for region in ADDITIONAL_COUNTRIES_IN_REGIONS:
        countries_in_region[region] = countries_in_region[region] + ADDITIONAL_COUNTRIES_IN_REGIONS[region]

    return countries_in_region


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
    """Add region aggregate for all regions.

    Regions are defined above, in REGIONS_TO_ADD.

    Parameters
    ----------
    data : pd.DataFrame
        Data.
    index_columns : list
        Name of index columns.
    index_columns : str
        Index columns.
    year_column : str
        Name of year column.

    Returns
    -------
    data : pd.DataFrame
        Data after adding regions.

    """
    data = data.copy()

    if aggregates is None:
        aggregates = {
            column: "sum" for column in data.columns if column not in index_columns
        }
    countries_in_regions = load_countries_in_regions()
    for region in regions:
        countries_in_region = countries_in_regions[region]
        data_region = data[data[country_column].isin(countries_in_region)]
        data_region = remove_overlapping_data_for_regions_and_members(
            df=data_region, known_overlaps=known_overlaps)        

        # Check that there are no other overlaps in the data.
        detect_overlapping_data_for_regions_and_members(
            df=data_region, list_of_countries=countries_in_region,
            index_columns=index_columns, known_overlaps=known_overlaps)

        # Add regions.
        data_region = geo.add_region_aggregates(
            df=data_region,
            region=region,
            country_col=country_column,
            year_col=year_column,
            aggregations=aggregates,
            countries_in_region=countries_in_region,
            countries_that_must_have_data=[],
            frac_allowed_nans_per_year=None,
            num_allowed_nans_per_year=None,
        )
        data = pd.concat(
            [data, data_region[data_region[country_column] == region]], ignore_index=True
        ).reset_index(drop=True)

    if region_codes is not None:
        # Add region codes to regions.
        if data[country_code_column].dtype == "category":
            data[country_code_column] = data[country_code_column].cat.add_categories(region_codes)
        for i, region in enumerate(regions):
            data.loc[data[country_column] == region, country_code_column] = region_codes[i]

    return data


def prepare_output_table(df: pd.DataFrame) -> catalog.Table:
    """Create a table with the processed data, ready to be in a garden dataset and to be uploaded to grapher (although
    additional metadata may need to be added to the table).

    Parameters
    ----------
    df : pd.DataFrame
        Processed BP data.

    Returns
    -------
    table : catalog.Table
        Table, ready to be added to a new garden dataset.

    """
    # Create new table.
    table = catalog.Table(df)

    # Replace spurious inf values by nan.
    table = table.replace([np.inf, -np.inf], np.nan)

    # Sort conveniently and add an index.
    table = (
        table.sort_values(["country", "year"])
        .reset_index(drop=True)
        .set_index(["country", "year"], verify_integrity=True)
        .astype({"country_code": "category"})
    )

    # Add metadata (e.g. unit) to each column.
    # Define unit names (these are the long and short unit names that will be shown in grapher).
    # The keys of the dictionary should correspond to units expected to be found in each of the variable names in table.
    short_unit_to_unit = {
        "TWh": "terawatt-hours",
        "kWh": "kilowatt-hours",
        "%": "%",
    }
    # Define number of decimal places to show (only relevant for grapher, not for the data).
    short_unit_to_num_decimals = {
        "TWh": 0,
        "kWh": 0,
    }
    for column in table.columns:
        table[column].metadata.title = column
        for short_unit in ["TWh", "kWh", "%"]:
            if short_unit in column:
                table[column].metadata.short_unit = short_unit
                table[column].metadata.unit = short_unit_to_unit[short_unit]
                table[column].metadata.display = {}
                if short_unit in short_unit_to_num_decimals:
                    table[column].metadata.display[
                        "numDecimalPlaces"
                    ] = short_unit_to_num_decimals[short_unit]
                # Add the variable name without unit (only relevant for grapher).
                table[column].metadata.display["name"] = column.split(" (")[0]

    table = catalog.utils.underscore_table(table)

    return table


def fill_missing_values_with_previous_version(
    table: catalog.Table, table_old: catalog.Table
) -> catalog.Table:
    """Fill missing values in current data with values from the previous version of the dataset.

    Parameters
    ----------
    table : catalog.Table
        Processed data from current dataset.
    table_old : catalog.Table
        Processed data from previous dataset.

    Returns
    -------
    combined : catalog.Table
        Combined table, with data from the current data, but after filling missing values with data from the previous
        version of the dataset.

    """
    # For region aggregates, avoid filling nan with values from previous releases.
    # The reason is that aggregates each year may include data from different countries.
    # This is especially necessary in 2022 because regions had different definitions in 2021 (the ones by BP).
    # Remove region aggregates from the old table.
    table_old = table_old.reset_index().rename(columns={"entity_name": "country", "entity_code": "country_code"}).\
        drop(columns=["entity_id"])
    table_old = table_old[~table_old["country"].isin(list(REGIONS_TO_ADD))].reset_index(drop=True).\
        set_index(["country", "year"])    

    # Combine the current output table with the table from the previous version the dataset.
    combined = pd.merge(
        table,
        table_old.drop(columns="country_code"),
        left_index=True,
        right_index=True,
        how="left",
        suffixes=("", "_old"),
    )    

    # List the common columns that can be filled with values from the previous version.
    columns = [column for column in combined.columns if column.endswith("_old")]    

    # Fill missing values in the current table with values from the old table.
    for column_old in columns:
        column = column_old.replace("_old", "")
        combined[column] = combined[column].fillna(combined[column_old])
    # Remove columns from the old table.
    combined = combined.drop(columns=columns)

    # Transfer metadata from the table of the current dataset into the combined table.
    combined.metadata = deepcopy(table.metadata)    

    for column in combined.columns:
        try:
            combined[column].metadata = deepcopy(table[column].metadata)
        except KeyError:
            combined[column].metadata = deepcopy(table_old[column].metadata)

    # Sanity checks.
    assert len(combined) == len(table)
    assert set(table.columns) <= set(combined.columns)

    return combined


def amend_zero_filled_variables_for_region_aggregates(df):
    df = df.copy()

    # Fill the "* (zero filled)" variables (which were ignored when creating aggregates) with the new aggregate
    # data, and fill any possible nan with zero.
    zero_filled_variables = [column for column in df.columns if "(zero filled)" in column]
    original_variables = [column.replace(" (zero filled)", "") for column in df.columns if "(zero filled)" in column]
    select_regions = df["country"].isin(REGIONS_TO_ADD)    
    df.loc[select_regions, zero_filled_variables] = df[select_regions][original_variables].fillna(0).values

    return df


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load table from latest BP dataset.
    bp_table = catalog.find_one(
        BP_CATALOG_NAME,
        channels=["backport"],
        namespace=f"{BP_NAMESPACE_IN_CATALOG}@{BP_VERSION}",
    )

    # Load previous version of the BP energy mix dataset, that will be used at the end to fill missing values in the
    # current dataset.
    bp_table_old = catalog.find_one(
        BP_CATALOG_NAME_OLD,
        channels=["backport"],
        namespace=f"{BP_NAMESPACE_IN_CATALOG_OLD}@{BP_VERSION_OLD}",
    )

    #
    # Process data.
    #
    # Extract dataframe of BP data from table.
    bp_data = pd.DataFrame(bp_table).reset_index().\
        rename(columns={column: bp_table[column].metadata.title for column in bp_table.columns}).\
        rename(columns={"entity_name": "country", "entity_code": "country_code"}).drop(columns="entity_id")

    # Add region aggregates.
    df = add_region_aggregates(
        data=bp_data,
        regions=list(REGIONS_TO_ADD),
        index_columns=["country", "year", "country_code"],
        country_column="country",
        year_column="year",
        aggregates={column: "sum" for column in AGGREGATES_BY_SUM},
        known_overlaps=OVERLAPPING_DATA_TO_REMOVE_IN_AGGREGATES,
        region_codes=[REGIONS_TO_ADD[region]["country_code"] for region in REGIONS_TO_ADD],
    )

    # Fill nans with zeros for "* (zero filled)" variables for region aggregates (which were ignored).
    df = amend_zero_filled_variables_for_region_aggregates(df)

    # Prepare output data in a convenient way.
    table = prepare_output_table(df)

    # Fill missing values in current table with values from the previous dataset, when possible.
    combined = fill_missing_values_with_previous_version(
        table=table, table_old=bp_table_old
    )

    #
    # Save outputs.
    #
    # Initialize new garden dataset.
    dataset = catalog.Dataset.create_empty(dest_dir)
    # Add metadata to dataset.
    dataset.metadata.update_from_yaml(METADATA_FILE_PATH)
    # Create new dataset in garden.
    dataset.save()

    # Add table to the dataset.
    combined.metadata.title = dataset.metadata.title
    combined.metadata.description = dataset.metadata.description
    combined.metadata.dataset = dataset.metadata
    combined.metadata.short_name = dataset.metadata.short_name
    combined.metadata.primary_key = list(combined.index.names)
    dataset.add(combined, repack=True)
