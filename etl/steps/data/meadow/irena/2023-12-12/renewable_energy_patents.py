# TODO: This file is a duplicate of the previous step. It is not yet used in the dag and should be updated soon.

import pandas as pd
from owid.catalog import Dataset, Table, TableMeta
from owid.walden import Catalog as WaldenCatalog

from etl.steps.data.converters import convert_walden_metadata

# Details for input datasets.
WALDEN_VERSION = "2022-10-25"
WALDEN_DATASET_NAME = "renewable_energy_patents"
# Details for output dataset.
VERSION = WALDEN_VERSION

# Columns to use from raw data and how to rename them.
COLUMNS = {
    "Country/area": "country",
    "Year": "year",
    "Sector": "sector",
    "Technology": "technology",
    "Sub Technology": "sub_technology",
    "Number of Patents": "patents",
}


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load raw data from Walden.
    walden_ds = WaldenCatalog().find_one(namespace="irena", short_name=WALDEN_DATASET_NAME, version=WALDEN_VERSION)
    local_file = walden_ds.ensure_downloaded()
    df = pd.read_csv(local_file)

    #
    # Process data.
    #
    # Select and rename columns conveniently.
    df = df[list(COLUMNS)].rename(columns=COLUMNS)

    # Set an appropriate index and sort conveniently.
    df = df.set_index(["country", "year", "sector", "technology", "sub_technology"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create new dataset and reuse metadata form Walden.
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_walden_metadata(walden_ds)
    ds.metadata.version = VERSION

    # Create table with metadata from dataframe
    table_metadata = TableMeta(short_name=walden_ds.short_name, title=walden_ds.name, description=walden_ds.description)
    tb = Table(df, metadata=table_metadata)

    # Add table to new dataset and save dataset.
    ds.add(tb)
    ds.save()
