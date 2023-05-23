"""Process the BP Statistical Review of World Energy 2021.

This dataset was downloaded and processed by a previous version of
https://github.com/owid/importers/tree/master/bp_statreview

However, in this additional step we add region aggregates following OWID definitions of regions.
"""

from copy import deepcopy

import numpy as np
import pandas as pd
from owid import catalog
from shared import CURRENT_DIR, REGIONS_TO_ADD, add_region_aggregates

from etl.helpers import PathFinder

P = PathFinder(__file__)

# Namespace and short name for output dataset.
NAMESPACE = "bp"
# Path to metadata file for current dataset.
METADATA_FILE_PATH = CURRENT_DIR / "statistical_review.meta.yml"
# Original BP's Statistical Review dataset name in the OWID catalog (without the institution and year).
BP_CATALOG_NAME = "statistical_review_of_world_energy"
BP_BACKPORTED_DATASET_NAME = "dataset_5347_statistical_review_of_world_energy__bp__2021"
BP_NAMESPACE_IN_CATALOG = "bp_statreview"
BP_VERSION = 2021

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
    bp_ds: catalog.Dataset = P.load_dependency(BP_BACKPORTED_DATASET_NAME)
    bp_table = bp_ds[BP_BACKPORTED_DATASET_NAME]

    #
    # Process data.
    #
    # Extract dataframe of BP data from table.
    bp_data = (
        pd.DataFrame(bp_table)
        .reset_index()
        .rename(columns={column: bp_table[column].metadata.title for column in bp_table.columns})
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
        region_codes=[REGIONS_TO_ADD[region]["country_code"] for region in REGIONS_TO_ADD],
    )

    # Fill nans with zeros for "* (zero filled)" variables for region aggregates (which were ignored).
    df = amend_zero_filled_variables_for_region_aggregates(df)

    # Prepare output data in a convenient way.
    table = prepare_output_table(df, bp_table)

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
    table.metadata.title = dataset.metadata.title
    table.metadata.description = dataset.metadata.description
    table.metadata.dataset = dataset.metadata
    table.metadata.short_name = dataset.metadata.short_name
    table.metadata.primary_key = list(table.index.names)
    dataset.add(table, repack=True)
