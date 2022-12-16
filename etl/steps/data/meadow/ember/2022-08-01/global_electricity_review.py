"""Meadow step for Global Electricity Review (Ember, 2022).

"""

import pandas as pd
from owid.catalog import Dataset, Table, TableMeta
from owid.catalog.utils import underscore_table
from shared import VERSION, log

from etl.snapshot import Snapshot
from etl.steps.data.converters import convert_snapshot_metadata

# Details of dataset to export.
NAMESPACE = "ember"
DATASET_SHORT_NAME = "global_electricity_review"
# Details of walden dataset to import.
WALDEN_VERSION = "2022-07-25"


def run(dest_dir: str) -> None:
    log.info(f"{DATASET_SHORT_NAME}.start")

    # Retrieve raw data from walden.
    snap = Snapshot(f"{NAMESPACE}/{WALDEN_VERSION}/{DATASET_SHORT_NAME}.csv")
    df = pd.read_csv(snap.path)

    # Create new dataset, reuse walden metadata, and update metadata.
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_snapshot_metadata(snap.metadata)
    ds.metadata.version = VERSION
    ds.save()

    # Create table with metadata from walden.
    table_metadata = TableMeta(
        short_name=snap.metadata.short_name,
        title=snap.metadata.name,
        description=snap.metadata.description,
    )
    tb = Table(df, metadata=table_metadata)

    # Set appropriate indexes.
    tb = tb.set_index(["Area", "Year", "Variable", "Unit"], verify_integrity=True)

    # Underscore all table columns.
    tb = underscore_table(tb)

    # Add table to dataset.
    ds.add(tb)

    log.info(f"{DATASET_SHORT_NAME}.end")
