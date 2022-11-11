"""Prepare national land-use change emissions data (from one of the official excel files) of the Global Carbon Budget.

"""

import pandas as pd
from owid.catalog import Dataset, Table, TableMeta
from owid.catalog.utils import underscore_table
from owid.walden import Catalog as WaldenCatalog

from etl.steps.data.converters import convert_walden_metadata

# Conversion factor to change from million tonnes of carbon to tonnes of CO2.
MILLION_TONNES_OF_CARBON_TO_TONNES_OF_CO2 = 3.664 * 1e6

# Details of dataset(s) to be imported.
WALDEN_DATASET_NAME = "global_carbon_budget_land_use_change_emissions"
WALDEN_VERSION = "2022-11-11"
# Details of dataset to be exported.
MEADOW_VERSION = WALDEN_VERSION
MEADOW_DATASET_NAME = WALDEN_DATASET_NAME
MEADOW_TITLE = "Global Carbon Budget - National land-use change emissions"


def prepare_land_use_emissions(land_use_df: pd.DataFrame) -> pd.DataFrame:
    """Prepare data from a specific sheet of the land-use change data file.

    Parameters
    ----------
    land_use_df : pd.DataFrame
        Data from a specific sheet of the land-use change emissions data file.

    Returns
    -------
    land_use_df : pd.DataFrame
        Processed land-use change emissions data.

    """
    land_use_df = land_use_df.copy()

    # Extract quality flag from the zeroth row of the data.
    # Ignore nans (which happen when a certain country has no data).
    quality_flag = (
        land_use_df.drop(columns=land_use_df.columns[0])
        .loc[0]
        .dropna()
        .astype(int)
        .to_frame("quality_flag")
        .reset_index()
        .rename(columns={"index": "country"})
    )

    # Drop the first row, which is for quality factor (which we have already extracted).
    land_use_df = land_use_df.rename(columns={land_use_df.columns[0]: "year"}).drop(0)

    # Ignore countries that have no data.
    land_use_df = land_use_df.dropna(axis=1, how="all")

    # Restructure data to have a column for country and another for emissions.
    land_use_df = land_use_df.melt(id_vars="year", var_name="country", value_name="emissions")

    error = "Countries with emissions data differ from countries with quality flag."
    assert set(land_use_df["country"]) == set(quality_flag["country"]), error

    # Add quality factor as an additional column.
    land_use_df = pd.merge(land_use_df, quality_flag, how="left", on="country")

    # Convert units from megatonnes of carbon per year emissions to tonnes of CO2 per year.
    land_use_df["emissions"] *= MILLION_TONNES_OF_CARBON_TO_TONNES_OF_CO2

    # Set an index and sort row and columns conveniently.
    land_use_df = land_use_df.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    return land_use_df


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load national land-use change data file from walden.
    land_use_ds = WaldenCatalog().find_one(namespace="gcp", short_name=WALDEN_DATASET_NAME, version=WALDEN_VERSION)
    # Load production-based emissions from the national data file.
    land_use_df = pd.read_excel(land_use_ds.ensure_downloaded(), sheet_name="BLUE", skiprows=7)

    # Sanity check.
    error = "'BLUE' sheet in national land-use change data file has changed (consider changing 'skiprows')."
    assert land_use_df.columns[1] == "Afghanistan", error

    #
    # Process data.
    #
    # Prepare land-use change emissions data (including a column for quality flag).
    land_use_df = prepare_land_use_emissions(land_use_df=land_use_df)

    #
    # Save outputs.
    #
    # Create new dataset and reuse walden metadata (from any of the raw files).
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_walden_metadata(land_use_ds)
    ds.metadata.version = MEADOW_VERSION
    # Create tables with metadata.
    land_use_tb = Table(
        land_use_df,
        metadata=TableMeta(short_name="land_use_change_emissions", title="Land-use change emissions"),
    )
    # Ensure all columns are lower snake case.
    land_use_tb = underscore_table(land_use_tb)
    # Add table to new dataset.
    ds.add(land_use_tb)
    # Save dataset.
    ds.save()
