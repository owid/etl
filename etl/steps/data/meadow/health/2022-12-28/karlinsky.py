import datetime as dt

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import Names
from etl.snapshot import Snapshot
from etl.steps.data.converters import convert_snapshot_metadata

log = get_logger()

# naming conventions
N = Names(__file__)
SNAPSHOT_DATASET = "health/2022-12-28/karlinsky.csv"
MEADOW_VERSION = N.version


def run(dest_dir: str) -> None:
    log.info("karlinsky.start")

    # retrieve snapshot
    snap = Snapshot(SNAPSHOT_DATASET)
    df = pd.read_csv(snap.path)

    # create new dataset and reuse walden metadata
    ds = Dataset.create_empty(dest_dir, metadata=convert_snapshot_metadata(snap.metadata))
    ds.metadata.version = MEADOW_VERSION
    ds.metadata.date_accessed = dt.date.today().strftime("%Y-%m-%d")

    # create table with metadata from dataframe and underscore all columns
    tb = Table(df, short_name=snap.metadata.short_name, underscore=True)

    # add table to a dataset
    ds.add(tb)

    # finally save the dataset
    ds.save()

    log.info("karlinsky.end")
