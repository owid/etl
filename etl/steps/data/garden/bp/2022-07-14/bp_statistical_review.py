"""Process the BP Statistical Review of World Energy 2022.

For the moment, this dataset is downloaded and processed by
https://github.com/owid/importers/tree/master/bp_statreview

However, in this additional step we add region aggregates following OWID definitions of regions.

"""

from copy import deepcopy

import numpy as np
import pandas as pd

from owid import catalog
from shared import CURRENT_DIR, REGIONS_TO_ADD, add_region_aggregates

# Namespace and short name for output dataset.
NAMESPACE = "bp"
DATASET_SHORT_NAME = "bp_statistical_review"
# Path to metadata file for current dataset.
METADATA_FILE_PATH = CURRENT_DIR / "bp_statistical_review.meta.yml"
# Original BP's Statistical Review dataset name in the OWID catalog (without the institution and year).
BP_CATALOG_NAME = "statistical_review_of_world_energy"
BP_NAMESPACE_IN_CATALOG = "bp_statreview"
BP_VERSION = 2022
# Previous BP's Statistical Review dataset.
# It will be used to fill missing data in the new dataset.
BP_CATALOG_NAME_OLD = "statistical_review_of_world_energy"
BP_NAMESPACE_IN_CATALOG_OLD = "bp_statreview"
BP_VERSION_OLD = 2021

# List of known overlaps between regions and member countries (or successor countries).
OVERLAPPING_DATA_TO_REMOVE_IN_AGGREGATES = [
    {
        "region": "USSR",
        "member": "Russia",
        "entity_to_make_nan": "region",
        "years": [1991, 1992, 1993, 1994, 1995, 1996],
        "variable": "Gas - Proved reserves",
    }
]

# True to ignore zeros when checking for overlaps between regions and member countries.
# This means that, if a region (e.g. USSR) and a member country or successor country (e.g. Russia) overlap, but in a
# variable that only has zeros, it will not be considered an overlap.
IGNORE_ZEROS_WHEN_CHECKING_FOR_OVERLAPPING_DATA = True

# Variables that can be summed when constructing region aggregates.
# Biofuels in Africa have a non-zero total, while there is no contribution from African countries.
# This causes that our aggregate for 'Africa' would be zero, while the original 'Africa (BP)' is not.
# Also, biodiesels are only given for continents and a few countries.
# For this reason we avoid creating aggregates for biofuels and biodiesels.
AGGREGATES_BY_SUM = [
    "Carbon Dioxide Emissions",
    "Coal - Reserves - Anthracite and bituminous",
    "Coal - Reserves - Sub-bituminous and lignite",
    "Coal - Reserves - Total",
    "Coal Consumption - EJ",
    "Coal Consumption - TWh",
    "Coal Production - EJ",
    "Coal Production - TWh",
    "Coal Production - Tonnes",
    "Cobalt Production-Reserves",
    "Elec Gen from Coal",
    "Elec Gen from Gas",
    "Elec Gen from Oil",
    "Electricity Generation",
    "Gas - Proved reserves",
    "Gas Consumption - Bcf",
    "Gas Consumption - Bcm",
    "Gas Consumption - EJ",
    "Gas Consumption - TWh",
    "Gas Production - Bcf",
    "Gas Production - Bcm",
    "Gas Production - EJ",
    "Gas Production - TWh",
    "Geo Biomass Other - EJ",
    "Geo Biomass Other - TWh",
    "Graphite Production-Reserves",
    "Hydro Consumption - EJ",
    "Hydro Consumption - TWh",
    "Hydro Generation - TWh",
    "Lithium Production-Reserves",
    "Nuclear Consumption - EJ",
    "Nuclear Consumption - TWh",
    "Nuclear Generation - TWh",
    "Oil - Proved reserves",
    "Oil - Refinery throughput",
    "Oil - Refining capacity",
    "Oil Consumption - Barrels",
    "Oil Consumption - EJ",
    "Oil Consumption - TWh",
    "Oil Consumption - Tonnes",
    "Oil Production - Barrels",
    "Oil Production - Crude Conds",
    "Oil Production - NGLs",
    "Oil Production - TWh",
    "Oil Production - Tonnes",
    "Primary Energy Consumption - EJ",
    "Primary Energy Consumption - TWh",
    "Renewables Consumption - EJ",
    "Renewables Consumption - TWh",
    "Renewables Power - EJ",
    "Renewables power - TWh",
    "Solar Capacity",
    "Solar Consumption - EJ",
    "Solar Consumption - TWh",
    "Solar Generation - TWh",
    "Total Liquids - Consumption",
    "Wind Capacity",
    "Wind Consumption - EJ",
    "Wind Consumption - TWh",
    "Wind Generation - TWh",
    # 'Biofuels Consumption - Kboed - Total',
    # 'Biofuels Consumption - Kboed - Biodiesel',
    # 'Biofuels Consumption - PJ - Total',
    # 'Biofuels Consumption - PJ - Biodiesel',
    # 'Biofuels Consumption - TWh - Total',
    # 'Biofuels Consumption - TWh - Biodiesel',
    # 'Biofuels Consumption - TWh - Biodiesel (zero filled)',
    # 'Biofuels Consumption - TWh - Total (zero filled)',
    # 'Biofuels Production - Kboed - Total',
    # 'Biofuels Production - PJ - Total',
    # 'Biofuels Production - TWh - Total',
    # 'Biofuels Production - Kboed - Biodiesel',
    # 'Biofuels Production - PJ - Biodiesel',
    # 'Biofuels Production - TWh - Biodiesel',
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
    # 'Rare Earth Production-Reserves',
    # 'Solar Consumption - TWh (zero filled)',
    # 'Wind Consumption - TWh (zero filled)',
]


def prepare_output_table(df: pd.DataFrame, bp_table: catalog.Table) -> catalog.Table:
    """Create a table with the processed data, ready to be in a garden dataset and to be uploaded to grapher (although
    additional metadata may need to be added to the table).

    Parameters
    ----------
    df : pd.DataFrame
        Processed BP data.
    bp_table : catalog.Table
        Original table of BP statistical review data (used to transfer its metadata to the new table).

    Returns
    -------
    table : catalog.Table
        Table, ready to be added to a new garden dataset.

    """
    # Create new table.
    table = catalog.Table(df).copy()

    # Replace spurious inf values by nan.
    table = table.replace([np.inf, -np.inf], np.nan)

    # Sort conveniently and add an index.
    table = (
        table.sort_values(["country", "year"])
        .reset_index(drop=True)
        .set_index(["country", "year"], verify_integrity=True)
        .astype({"country_code": "category"})
    )

    # Convert column names to lower, snake case.
    table = catalog.utils.underscore_table(table)

    # Get the table metadata from the original table.
    table.metadata = deepcopy(bp_table.metadata)

    # Get the metadata of each variable from the original table.
    for column in table.drop(columns="country_code").columns:
        table[column].metadata = deepcopy(bp_table[column].metadata)

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
    table_old = (
        table_old.reset_index()
        .rename(columns={"entity_name": "country", "entity_code": "country_code"})
        .drop(columns=["entity_id"])
    )
    table_old = (
        table_old[~table_old["country"].isin(list(REGIONS_TO_ADD))]
        .reset_index(drop=True)
        .set_index(["country", "year"])
    )

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
    # When that is not possible (for columns that were only in the old but not in the new table),
    # get the metadata from the old table.

    for column in combined.columns:
        try:
            combined[column].metadata = deepcopy(table[column].metadata)
        except KeyError:
            combined[column].metadata = deepcopy(table_old[column].metadata)

    # Sanity checks.
    assert len(combined) == len(table)
    assert set(table.columns) <= set(combined.columns)

    return combined


def amend_zero_filled_variables_for_region_aggregates(df: pd.DataFrame) -> pd.DataFrame:
    """Fill the "* (zero filled)" variables (which were ignored when creating aggregates) with the new aggregate data,
    and fill any possible nan with zeros.

    Parameters
    ----------
    df : pd.DataFrame
        Data after having created region aggregates (which ignore '* (zero filled)' variables).

    Returns
    -------
    df : pd.DataFrame
        Data after amending zero filled variables for region aggregates.

    """
    df = df.copy()

    zero_filled_variables = [
        column for column in df.columns if "(zero filled)" in column
    ]
    original_variables = [
        column.replace(" (zero filled)", "")
        for column in df.columns
        if "(zero filled)" in column
    ]
    select_regions = df["country"].isin(REGIONS_TO_ADD)
    df.loc[select_regions, zero_filled_variables] = (
        df[select_regions][original_variables].fillna(0).values
    )

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
    bp_data = (
        pd.DataFrame(bp_table)
        .reset_index()
        .rename(
            columns={
                column: bp_table[column].metadata.title for column in bp_table.columns
            }
        )
        .rename(columns={"entity_name": "country", "entity_code": "country_code"})
        .drop(columns="entity_id")
    )

    # Add region aggregates.
    df = add_region_aggregates(
        data=bp_data,
        regions=list(REGIONS_TO_ADD),
        index_columns=["country", "year", "country_code"],
        country_column="country",
        year_column="year",
        aggregates={column: "sum" for column in AGGREGATES_BY_SUM},
        known_overlaps=OVERLAPPING_DATA_TO_REMOVE_IN_AGGREGATES,  # type: ignore
        region_codes=[
            REGIONS_TO_ADD[region]["country_code"] for region in REGIONS_TO_ADD
        ],
    )

    # Fill nans with zeros for "* (zero filled)" variables for region aggregates (which were ignored).
    df = amend_zero_filled_variables_for_region_aggregates(df)

    # Prepare output data in a convenient way.
    table = prepare_output_table(df, bp_table)

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
