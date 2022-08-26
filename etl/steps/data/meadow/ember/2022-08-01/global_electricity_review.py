"""Meadow step for Global Electricity Review (Ember, 2022).

"""

import pandas as pd
from owid.catalog import Dataset, Table, TableMeta
from owid.catalog.utils import underscore_table
from owid.walden import Catalog as WaldenCatalog
from shared import VERSION, log

from etl.steps.data.converters import convert_walden_metadata

# Details of dataset to export.
NAMESPACE = "ember"
DATASET_SHORT_NAME = "global_electricity_review"
# Details of walden dataset to import.
WALDEN_VERSION = "2022-07-25"


def run(dest_dir: str) -> None:
    log.info(f"{DATASET_SHORT_NAME}.start")

    # Retrieve raw data from walden.
    walden_ds = WaldenCatalog().find_one(namespace=NAMESPACE, short_name=DATASET_SHORT_NAME, version=WALDEN_VERSION)
    local_file = walden_ds.ensure_downloaded()
    df = pd.read_csv(local_file)

    # Create new dataset, reuse walden metadata, and update metadata.
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_walden_metadata(walden_ds)
    ds.metadata.version = VERSION
    ds.save()

    # Create table with metadata from walden.
    table_metadata = TableMeta(
        short_name=walden_ds.short_name,
        title=walden_ds.name,
        description=walden_ds.description,
    )
    tb = Table(df, metadata=table_metadata)

    # Set appropriate indexes.
    tb = tb.set_index(["Area", "Year", "Variable", "Unit"], verify_integrity=True)

    # Underscore all table columns.
    tb = underscore_table(tb)

    # Add table to dataset.
    ds.add(tb)

    log.info(f"{DATASET_SHORT_NAME}.end")
