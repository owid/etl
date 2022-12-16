from typing import Tuple

import pandas as pd
from owid.catalog import Dataset, Table, TableMeta
from owid.catalog.utils import underscore_table
from structlog import get_logger

from etl.helpers import Names
from etl.snapshot import Snapshot
from etl.steps.data.converters import convert_snapshot_metadata

log = get_logger()

# naming conventions
N = Names(__file__)


# Dataset details from Walden
NAMESPACE = "papers"
SHORT_NAME = "zijdeman_et_al_2015"
VERSION_WALDEN = "2022-11-01"
# Meadow version
VERSION_MEADOW = "2022-11-03"


def run(dest_dir: str) -> None:
    log.info("zijdeman_et_al_2015.start")

    # retrieve raw data from walden
    snap = Snapshot(f"{NAMESPACE}/{VERSION_WALDEN}/{SHORT_NAME}.csv")
    local_file = str(snap.path)

    # Load data
    df, metadata = load_data(str(local_file))
    # Create table
    tb = make_table(df, snap)
    tb_metadata = make_table_metadata(metadata, snap)

    # initialize meadow dataset
    ds = init_meadow_dataset(dest_dir, snap)
    # add table to a dataset
    ds.add(tb)
    ds.add(tb_metadata)
    # finally save the dataset
    ds.save()

    log.info("zijdeman_et_al_2015.end")


def load_data(path: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Load data from Excel file."""
    df = pd.read_excel(path, sheet_name=None)
    data = df["Data Long Format"]
    metadata = df["Metadata"]
    return data, metadata


def init_meadow_dataset(dest_dir: str, snap: Snapshot) -> Dataset:
    """Initialize meadow dataset."""
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_snapshot_metadata(snap.metadata)
    ds.metadata.version = VERSION_MEADOW
    return ds


def make_table(df: pd.DataFrame, snap: Snapshot) -> Table:
    """Create table from dataframe and Walden metadata."""
    table_metadata = TableMeta(
        short_name=snap.metadata.short_name,
        title=snap.metadata.name,
        description=snap.metadata.description,
    )
    tb = Table(df, metadata=table_metadata)
    tb = underscore_table(tb)
    return tb


def make_table_metadata(df: pd.DataFrame, snap: Snapshot) -> Table:
    """Create metadata table from dataframe and Walden metadata."""
    table_metadata = TableMeta(
        short_name="metadata",
        title=f"{snap.metadata.name} (metadata)",
        description="Metadata for the dataset.",
    )
    tb = Table(df, metadata=table_metadata)
    tb = underscore_table(tb)
    return tb
