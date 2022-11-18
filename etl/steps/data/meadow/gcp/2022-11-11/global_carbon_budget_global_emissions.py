"""Prepare global emissions data (from one of the official excel files) of the Global Carbon Budget.

The resulting dataset will have one table of historical global emissions, where fossil and land-use change emissions are
separate variables. Bunker fuel emissions are not included as a separate variable (but their contribution is included as
part of fossil emissions).

"""

import pandas as pd
from owid.catalog import Dataset, Table, TableMeta
from owid.catalog.utils import underscore_table
from owid.walden import Catalog as WaldenCatalog

from etl.steps.data.converters import convert_walden_metadata

# Conversion factor to change from billion tonnes of carbon to tonnes of CO2.
BILLION_TONNES_OF_CARBON_TO_TONNES_OF_CO2 = 3.664 * 1e9

# Details of dataset(s) to be imported.
WALDEN_DATASET_NAME = "global_carbon_budget_global_emissions"
WALDEN_VERSION = "2022-11-11"
# Details of dataset to be exported.
MEADOW_VERSION = WALDEN_VERSION
MEADOW_DATASET_NAME = WALDEN_DATASET_NAME
MEADOW_TITLE = "Global Carbon Budget - Global emissions"


def prepare_historical_budget(df: pd.DataFrame) -> pd.DataFrame:
    """Select variables and prepare the historical budget sheet of GCB's raw global data file.

    Parameters
    ----------
    df : pd.DataFrame
        Historical budget sheet of GCB's raw global data file.

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


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load historical budget data from the global data file in walden.
    global_ds = WaldenCatalog().find_one(namespace="gcp", short_name=WALDEN_DATASET_NAME, version=WALDEN_VERSION)
    historical_budget_df = pd.read_excel(global_ds.ensure_downloaded(), sheet_name="Historical Budget", skiprows=15)

    # Sanity check.
    error = "'Historical Budget' sheet in global data file has changed (consider changing 'skiprows')."
    assert historical_budget_df.columns[0] == "Year", error

    #
    # Process data.
    #
    # Prepare historical budget data.
    historical_budget_df = prepare_historical_budget(df=historical_budget_df)

    #
    # Save outputs.
    #
    # Create new dataset and reuse walden metadata (from any of the raw files).
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_walden_metadata(global_ds)
    ds.metadata.version = MEADOW_VERSION
    # Create tables with metadata.
    historical_budget_tb = Table(
        historical_budget_df, metadata=TableMeta(short_name="historical_emissions", title="Historical emissions")
    )
    # Ensure all columns are lower snake case.
    historical_budget_tb = underscore_table(historical_budget_tb)
    # Add table to new dataset.
    ds.add(historical_budget_tb)
    # Save dataset.
    ds.save()
