"""This step just loads additional variables that are currently not included in the Global Carbon Budget (GCB) dataset
(which was created in importers).

In the future (next time GCB dataset is updated and moved to ETL), a newer version of this step should gather all
required data from walden.

"""

import pandas as pd
from owid.catalog import Dataset, Table, TableMeta
from owid.catalog.utils import underscore_table
from owid.walden import Catalog as WaldenCatalog

from etl.helpers import Names
from etl.steps.data.converters import convert_walden_metadata

# Conversion factor to change from tonnes of carbon to tonnes of CO2.
CARBON_TO_CO2 = 3.664

# Details of dataset(s) to be imported.
WALDEN_DATASET_NAME = "global_carbon_budget_global"
WALDEN_VERSION = "2022-09-29"
# Details of dataset to be exported.
MEADOW_VERSION = "2022-09-29"
MEADOW_DATASET_NAME = "global_carbon_budget_additional"
MEADOW_TITLE = "Global Carbon Budget - Additional variables"


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load raw data from walden.
    walden_ds = WaldenCatalog().find_one(namespace="gcp", short_name=WALDEN_DATASET_NAME, version=WALDEN_VERSION)
    local_file = walden_ds.ensure_downloaded()
    
    # For now, load only the historical budget sheet.
    df = pd.read_excel(local_file, sheet_name="Historical Budget", skiprows=15)
    assert df.columns[0] == "Year", "The structure of the file has changed (consider changing skiprows)."

    #
    # Process data.
    #
    # Columns to select and how to rename them.
    columns = {
        "Year": "year",
        "fossil emissions excluding carbonation": 'fossil emissions excluding carbonation',
        "land-use change emissions": "land-use change emissions",
    }
    df = df[list(columns)].rename(columns=columns)
    # Convert units from gigatonnes of carbon per year emissions to gigatonnes of CO2 per year.
    for column in df.drop(columns="year").columns:
        df[column] *= CARBON_TO_CO2

    #
    # Save outputs.
    #
    # Create new dataset and reuse walden metadata.
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_walden_metadata(walden_ds)
    ds.metadata.version = MEADOW_VERSION
    # Create table with metadata from dataframe.
    table_metadata = TableMeta(
        short_name=MEADOW_DATASET_NAME,
        title=MEADOW_TITLE,
        description=walden_ds.description,
    )
    tb = Table(df, metadata=table_metadata)
    # Ensure all columns are lower snake case.
    tb = underscore_table(tb)
    # Add table to new dataset.
    ds.add(tb)
    # Save dataset.
    ds.save()
