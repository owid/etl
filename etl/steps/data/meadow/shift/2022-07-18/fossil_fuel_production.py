"""Meadow step for Shift data on energy production from fossil fuels.

"""
from pathlib import Path

import pandas as pd
from owid.catalog import Dataset, Table, TableMeta
from owid.catalog.utils import underscore_table
from structlog import get_logger

from etl.snapshot import Snapshot
from etl.snapshot import Snapshot
from etl.steps.data.converters import convert_snapshot_metadata

log = get_logger()

NAMESPACE = "shift"
DATASET_SHORT_NAME = "fossil_fuel_production"
VERSION = Path(__file__).parent.name


def run(dest_dir: str) -> None:
    log.info(f"{DATASET_SHORT_NAME}.start")

    # Load data from walden.
    snap = Snapshot(f"{NAMESPACE}/{}/{}"), short_name=DATASET_SHORT_NAME, version=VERSION)
    local_file = str(snap.path)
    df = pd.read_csv(local_file)

    # Create new dataset using metadata from walden.
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_snapshot_metadata(snap.metadata)
    ds.save()

    # Create a table in the dataset with the same metadata as the dataset.
    table_metadata = TableMeta(
        short_name=snap.metadata.short_name,
        title=snap.metadata.name,
        description=snap.metadata.description,
    )
    tb = Table(df, metadata=table_metadata)

    # Ensure all columns are lower-case and snake-case.
    tb = underscore_table(tb)

    # Add table to a dataset.
    ds.add(tb)

    log.info(f"{DATASET_SHORT_NAME}.end")
