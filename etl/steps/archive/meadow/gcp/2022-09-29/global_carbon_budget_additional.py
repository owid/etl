"""This step just loads additional variables that are currently not included in the Global Carbon Budget (GCB) dataset
(which was created in importers).

In the future (next time GCB dataset is updated and moved to ETL), a newer version of this step should gather all
required data from walden.

"""

import pandas as pd
from owid.catalog import Dataset, Table, TableMeta
from owid.catalog.utils import underscore_table
from owid.walden import Catalog as WaldenCatalog

from etl.steps.data.converters import convert_walden_metadata

# Conversion factor to change from million tonnes of carbon to tonnes of CO2.
MILLION_TONNES_OF_CARBON_TO_TONNES_OF_CO2 = 3.664 * 1e6
# Conversion factor to change from billion tonnes of carbon to tonnes of CO2.
BILLION_TONNES_OF_CARBON_TO_TONNES_OF_CO2 = 3.664 * 1e9

# Details of dataset(s) to be imported.
WALDEN_GLOBAL_DATASET_NAME = "global_carbon_budget_global"
WALDEN_NATIONAL_DATASET_NAME = "global_carbon_budget_national"
WALDEN_VERSION = "2022-09-29"
# Details of dataset to be exported.
MEADOW_VERSION = "2022-09-29"
MEADOW_DATASET_NAME = "global_carbon_budget_additional"
MEADOW_TITLE = "Global Carbon Budget - Additional variables"


def prepare_historical_budget(df: pd.DataFrame) -> pd.DataFrame:
    """Select variables and prepare the historical budget sheet of GCB's raw national data file.

    Parameters
    ----------
    df : pd.DataFrame
        Historical budget sheet of GCB's raw national data file.

    Returns
    -------
    df : pd.DataFrame
        Historical budget after selecting variables and processing them.

    """
    # Columns to select in historical budget and how to rename them.
    columns = {
        "Year": "year",
        "fossil emissions excluding carbonation": "global_fossil_emissions",
        "land-use change emissions": "global_land_use_change_emissions",
    }
    df = df[list(columns)].rename(columns=columns)

    # Convert units from gigatonnes of carbon per year emissions to tonnes of CO2 per year.
    for column in df.drop(columns="year").columns:
        df[column] *= BILLION_TONNES_OF_CARBON_TO_TONNES_OF_CO2

    # Add column for country (to be able to combine this with the national data).
    df["country"] = "World"

    # Set an index and sort row and columns conveniently.
    df = df.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    return df


def prepare_emissions(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
    """Select variables and prepare the territorial emissions or the consumption emissions sheet of
    GCB's raw global data file.

    Parameters
    ----------
    df : pd.DataFrame
        Territorial emissions (or consumption emissions) sheet of GCB's raw national data file.
    column_name : str
        Name to assign to emissions column to be generated.

    Returns
    -------
    df : pd.DataFrame
        Processed territorial (or consumption) emissions sheet of GCB's raw global data file.

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
    # Load historical budget data from the global data file in walden.
    global_ds = WaldenCatalog().find_one(namespace="gcp", short_name=WALDEN_GLOBAL_DATASET_NAME, version=WALDEN_VERSION)
    historical_budget_df = pd.read_excel(global_ds.ensure_downloaded(), sheet_name="Historical Budget", skiprows=15)
    error = "'Historical Budget' sheet in global data file has changed (consider changing 'skiprows')."
    assert historical_budget_df.columns[0] == "Year", error

    # Load national data file from walden.
    national_ds = WaldenCatalog().find_one(
        namespace="gcp", short_name=WALDEN_NATIONAL_DATASET_NAME, version=WALDEN_VERSION
    )
    # Load production-based emissions from the national data file.
    production_emissions_df = pd.read_excel(
        national_ds.ensure_downloaded(), sheet_name="Territorial Emissions", skiprows=11
    )
    error = "'Territorial Emissions' sheet in national data file has changed (consider changing 'skiprows')."
    assert production_emissions_df.columns[1] == "Afghanistan", error
    # Load consumption-based emissions from the national data file.
    consumption_emissions_df = pd.read_excel(
        national_ds.ensure_downloaded(), sheet_name="Consumption Emissions", skiprows=8
    )
    error = "'Consumption Emissions' sheet in national data file has changed (consider changing 'skiprows')."
    assert consumption_emissions_df.columns[1] == "Afghanistan", error

    #
    # Process data.
    #
    # Prepare historical budget data.
    historical_budget_df = prepare_historical_budget(df=historical_budget_df)

    # Prepare production and consumption based emissions data.
    production_emissions_df = prepare_emissions(df=production_emissions_df, column_name="production_emissions")
    consumption_emissions_df = prepare_emissions(df=consumption_emissions_df, column_name="consumption_emissions")

    #
    # Save outputs.
    #
    # Create new dataset and reuse walden metadata (from any of the raw files).
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_walden_metadata(global_ds)
    ds.metadata.version = MEADOW_VERSION
    ds.metadata.short_name = MEADOW_DATASET_NAME
    # Create tables with metadata.
    consumption_emissions_tb = Table(
        consumption_emissions_df,
        metadata=TableMeta(short_name="consumption_emissions", title="Consumption-based emissions"),
    )
    production_emissions_tb = Table(
        production_emissions_df,
        metadata=TableMeta(short_name="production_emissions", title="Production-based emissions"),
    )
    historical_budget_tb = Table(
        historical_budget_df, metadata=TableMeta(short_name="historical_emissions", title="Historical emissions")
    )
    # Ensure all columns are lower snake case.
    consumption_emissions_tb = underscore_table(consumption_emissions_tb)
    production_emissions_tb = underscore_table(production_emissions_tb)
    historical_budget_tb = underscore_table(historical_budget_tb)
    # Add tables to new dataset.
    ds.add(consumption_emissions_tb)
    ds.add(production_emissions_tb)
    ds.add(historical_budget_tb)
    # Save dataset.
    ds.save()
