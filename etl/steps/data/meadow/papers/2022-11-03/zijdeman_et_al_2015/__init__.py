from typing import Tuple

import pandas as pd
from owid.catalog import Dataset, Table, TableMeta
from owid.catalog.utils import underscore_table
from owid.walden import Catalog as WaldenCatalog
from structlog import get_logger

from etl.helpers import PathFinder
from etl.steps.data.converters import convert_walden_metadata

log = get_logger()

# naming conventions
N = PathFinder(__file__)


# Dataset details from Walden
NAMESPACE = "papers"
SHORT_NAME = "zijdeman_et_al_2015"
VERSION_WALDEN = "2022-11-01"
# Meadow version
VERSION_MEADOW = "2022-11-03"


def run(dest_dir: str) -> None:
    log.info("zijdeman_et_al_2015.start")

    # retrieve raw data from walden
    walden_ds = WaldenCatalog().find_one(namespace=NAMESPACE, short_name=SHORT_NAME, version=VERSION_WALDEN)
    local_file = walden_ds.ensure_downloaded()

    # Load data
    df, metadata = load_data(local_file)
    # Create table
    tb = make_table(df, walden_ds)
    tb_metadata = make_table_metadata(metadata, walden_ds)

    # initialize meadow dataset
    ds = init_meadow_dataset(dest_dir, walden_ds)
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


def init_meadow_dataset(dest_dir: str, walden_ds: WaldenCatalog) -> Dataset:
    """Initialize meadow dataset."""
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_walden_metadata(walden_ds)
    ds.metadata.version = VERSION_MEADOW
    return ds


def make_table(df: pd.DataFrame, walden_ds: WaldenCatalog) -> Table:
    """Create table from dataframe and Walden metadata."""
    table_metadata = TableMeta(
        short_name=walden_ds.short_name,
        title=walden_ds.name,
        description=walden_ds.description,
    )
    tb = Table(df, metadata=table_metadata)
    tb = underscore_table(tb)
    return tb


def make_table_metadata(df: pd.DataFrame, walden_ds: WaldenCatalog) -> Table:
    """Create metadata table from dataframe and Walden metadata."""
    table_metadata = TableMeta(
        short_name="metadata",
        title=f"{walden_ds.name} (metadata)",
        description="Metadata for the dataset.",
    )
    tb = Table(df, metadata=table_metadata)
    tb = underscore_table(tb)
    return tb
