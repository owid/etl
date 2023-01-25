import pandas as pd
from owid.catalog import Dataset, Table, TableMeta
from owid.catalog.utils import underscore_table
from structlog import get_logger

from etl.helpers import PathFinder
from etl.snapshot import Snapshot
from etl.steps.data.converters import convert_snapshot_metadata

log = get_logger()

# naming conventions
N = PathFinder(__file__)


SHORT_NAME = "undp_hdr"
SNAP_VERSION = "2022-11-29"
SNAP_DATA = f"un/{SNAP_VERSION}/{SHORT_NAME}.csv"
SNAP_METADATA = f"un/{SNAP_VERSION}/{SHORT_NAME}.xlsx"
MEADOW_VERSION = "2022-11-29"


def run(dest_dir: str) -> None:
    log.info("undp_hdr.start")

    # retrieve data snapshot
    snap = Snapshot(SNAP_DATA)
    df = pd.read_csv(snap.path)
    # build table
    tb = make_table(snap, df)
    # create new dataset and reuse walden metadata
    ds = init_dataset(dest_dir, snap)
    # add table to a dataset
    ds.add(tb)

    # retrieve metadata snapshot
    snap = Snapshot(SNAP_METADATA)
    df_metadata = pd.read_excel(snap.path)
    tb_metadata = make_table_metadata(snap, df_metadata)
    ds.add(tb_metadata)

    ds.save()
    log.info("undp_hdr.end")


def make_table(snap: Snapshot, df: pd.DataFrame) -> Table:
    # create table with metadata from dataframe
    table_metadata = TableMeta(
        short_name=snap.metadata.short_name,
        title=snap.metadata.name,
        description=snap.metadata.description,
    )
    tb = Table(df, metadata=table_metadata)

    # underscore all table columns
    tb = underscore_table(tb)

    # tb.update_metadata_from_yaml(N.metadata_path, SHORT_NAME)
    return tb


def make_table_metadata(snap: Snapshot, df: pd.DataFrame) -> Table:
    # create table with metadata from dataframe
    table_metadata = TableMeta(
        short_name=f"{snap.metadata.short_name} (metadata)",
        title=f"{snap.metadata.name} (metadata)",
        description="Metadata for the UNDP Human Development.",
    )
    tb = Table(df, metadata=table_metadata)

    # underscore all table columns
    tb = underscore_table(tb)
    tb = tb.astype({"time_series": "str"})
    # tb.update_metadata_from_yaml(N.metadata_path, SHORT_NAME)
    return tb


def init_dataset(dest_dir: str, snap: Snapshot) -> Dataset:
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_snapshot_metadata(snap.metadata)
    ds.metadata.version = MEADOW_VERSION

    # ds.metadata.update_from_yaml(N.metadata_path, if_source_exists="replace")
    return ds
