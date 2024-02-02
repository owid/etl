"""Meadow step for Shift data on energy production from fossil fuels.

"""
from pathlib import Path

import pandas as pd
from owid.catalog import Dataset, Table, TableMeta
from owid.catalog.utils import underscore_table
from owid.walden import Catalog as WaldenCatalog
from structlog import get_logger

from etl.steps.data.converters import convert_walden_metadata

log = get_logger()

NAMESPACE = "shift"
DATASET_SHORT_NAME = "fossil_fuel_production"
VERSION = Path(__file__).parent.name


def run(dest_dir: str) -> None:
    log.info(f"{DATASET_SHORT_NAME}.start")

    # Load data from walden.
    walden_ds = WaldenCatalog().find_one(namespace=NAMESPACE, short_name=DATASET_SHORT_NAME, version=VERSION)
    local_file = walden_ds.ensure_downloaded()
    df = pd.read_csv(local_file)

    # Create new dataset using metadata from walden.
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_walden_metadata(walden_ds)
    ds.save()

    # Create a table in the dataset with the same metadata as the dataset.
    table_metadata = TableMeta(
        short_name=walden_ds.short_name,
        title=walden_ds.name,
        description=walden_ds.description,
    )
    tb = Table(df, metadata=table_metadata)

    # Ensure all columns are lower-case and snake-case.
    tb = underscore_table(tb)

    # Add table to a dataset.
    ds.add(tb)

    log.info(f"{DATASET_SHORT_NAME}.end")
