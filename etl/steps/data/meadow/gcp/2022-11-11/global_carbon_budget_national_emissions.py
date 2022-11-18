"""Prepare national fossil emissions data (from one of the official excel files) of the Global Carbon Budget.

The resulting dataset will have one table for production-based emissions, and another for consumption-based emissions.
Bunker emissions (which should be the same in both tables) is included as a separate country (called "Bunkers").

"""

import pandas as pd
from owid.catalog import Dataset, Table, TableMeta
from owid.catalog.utils import underscore_table
from owid.walden import Catalog as WaldenCatalog

from etl.steps.data.converters import convert_walden_metadata

# Conversion factor to change from million tonnes of carbon to tonnes of CO2.
MILLION_TONNES_OF_CARBON_TO_TONNES_OF_CO2 = 3.664 * 1e6

# Details of dataset(s) to be imported.
WALDEN_DATASET_NAME = "global_carbon_budget_national_emissions"
WALDEN_VERSION = "2022-11-11"
# Details of dataset to be exported.
MEADOW_VERSION = WALDEN_VERSION
MEADOW_DATASET_NAME = "global_carbon_budget_national_emissions"
MEADOW_TITLE = "Global Carbon Budget - National emissions"


def prepare_national_emissions(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
    """Select variables and prepare the territorial emissions (or the consumption emissions) sheet of GCB's raw national
    data file.

    Parameters
    ----------
    df : pd.DataFrame
        Territorial emissions (or consumption emissions) sheet of GCB's raw national data file.
    column_name : str
        Name to assign to emissions column to be generated.

    Returns
    -------
    df : pd.DataFrame
        Processed territorial (or consumption) emissions sheet of GCB's raw national data file.

    """
    df = df.copy()

    # The zeroth column is expected to be year.
    df = df.rename(columns={df.columns[0]: "year"})

    # Each column represents a country; then the final columns are regions, "Bunkers", and "Statistical Difference".
    # Keep "Bunkers", but remove "Statistical Difference" (which is almost completely empty).
    # In fact "Bunkers" is a global variable (I don't know why it is included at the national level), but this will be
    # handled at the garden step.

    # Remove unnecessary column.
    df = df.drop(columns=["Statistical Difference"])

    # Convert from wide to long format dataframe.
    df = df.melt(id_vars=["year"]).rename(columns={"variable": "country", "value": column_name})

    # Convert units from megatonnes of carbon per year emissions to tonnes of CO2 per year.
    for column in df.drop(columns=["country", "year"]).columns:
        df[column] *= MILLION_TONNES_OF_CARBON_TO_TONNES_OF_CO2

    # Set an index and sort row and columns conveniently.
    df = df.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    return df


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load national data file from walden.
    national_ds = WaldenCatalog().find_one(namespace="gcp", short_name=WALDEN_DATASET_NAME, version=WALDEN_VERSION)
    # Load production-based emissions from the national data file.
    production_emissions_df = pd.read_excel(
        national_ds.ensure_downloaded(), sheet_name="Territorial Emissions", skiprows=11
    )

    # Sanity check.
    error = "'Territorial Emissions' sheet in national data file has changed (consider changing 'skiprows')."
    assert production_emissions_df.columns[1] == "Afghanistan", error

    # Load consumption-based emissions from the national data file.
    consumption_emissions_df = pd.read_excel(
        national_ds.ensure_downloaded(), sheet_name="Consumption Emissions", skiprows=8
    )

    # Sanity check.
    error = "'Consumption Emissions' sheet in national data file has changed (consider changing 'skiprows')."
    assert consumption_emissions_df.columns[1] == "Afghanistan", error

    #
    # Process data.
    #
    # Prepare production-based and consumption-based emissions data.
    production_emissions_df = prepare_national_emissions(df=production_emissions_df, column_name="production_emissions")
    consumption_emissions_df = prepare_national_emissions(
        df=consumption_emissions_df, column_name="consumption_emissions"
    )

    #
    # Save outputs.
    #
    # Create new dataset and reuse walden metadata (from any of the raw files).
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_walden_metadata(national_ds)
    ds.metadata.version = MEADOW_VERSION
    # Create tables with metadata.
    consumption_emissions_tb = Table(
        consumption_emissions_df,
        metadata=TableMeta(short_name="consumption_emissions", title="Consumption-based emissions"),
    )
    production_emissions_tb = Table(
        production_emissions_df,
        metadata=TableMeta(short_name="production_emissions", title="Production-based emissions"),
    )
    # Ensure all columns are lower snake case.
    consumption_emissions_tb = underscore_table(consumption_emissions_tb)
    production_emissions_tb = underscore_table(production_emissions_tb)
    # Add tables to new dataset.
    ds.add(consumption_emissions_tb)
    ds.add(production_emissions_tb)
    # Save dataset.
    ds.save()
