"""Generate a dataset of aviation statistics using data from the Aviation Safety Network.

"""

import pandas as pd
from owid.catalog import Dataset, Table, TableMeta
from owid.catalog.utils import underscore_table
from owid.walden import Catalog as WaldenCatalog

from etl.steps.data.converters import convert_walden_metadata

NAMESPACE = "aviation_safety_network"
# Details for input datasets.
WALDEN_DATASET_NAME = "aviation_statistics"
WALDEN_VERSION = "2022-10-14"
# Details for output dataset.
MEADOW_VERSION = WALDEN_VERSION
MEADOW_DATASET_NAME = "aviation_statistics"
MEADOW_DATASET_TITLE = "Aviation statistics"


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Get data (statistics by period and by nature) from Walden.
    walden_ds = WaldenCatalog().find_one(namespace=NAMESPACE, short_name=WALDEN_DATASET_NAME, version=WALDEN_VERSION)
    local_file = walden_ds.ensure_downloaded()
    df = pd.read_csv(local_file).rename(columns={"year": "year"})

    #
    # Process data.
    #
    # Add a country column (that only contains "World").
    df["country"] = "World"

    # Set an appropriate index and sort conveniently.
    df = df.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    #
    # Save outputs.
    #
    # Create a new dataset and reuse Walden metadata.
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_walden_metadata(walden_ds)
    ds.metadata.version = MEADOW_VERSION

    # Create a table with metadata.
    table_metadata = TableMeta(
        short_name=MEADOW_DATASET_NAME, title=MEADOW_DATASET_TITLE, description=walden_ds.description
    )
    tb = Table(df, metadata=table_metadata)

    # Ensure all columns are snake-case and underscore.
    tb = underscore_table(tb)

    # Add table to new Meadow dataset.
    ds.add(tb)

    # Save the new Meadow dataset.
    ds.save()
