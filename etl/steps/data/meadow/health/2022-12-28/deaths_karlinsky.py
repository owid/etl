import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import PathFinder
from etl.snapshot import Snapshot
from etl.steps.data.converters import convert_snapshot_metadata

log = get_logger()

# naming conventions
N = PathFinder(__file__)
SNAPSHOT_DATASET = "health/2022-12-28/deaths_karlinsky.csv"
MEADOW_VERSION = N.version


def run(dest_dir: str) -> None:
    log.info("deaths_karlinsky.start")

    # retrieve snapshot
    snap = Snapshot(SNAPSHOT_DATASET)
    df = pd.read_csv(snap.path)

    # create new dataset and reuse walden metadata
    ds = Dataset.create_empty(dest_dir, metadata=convert_snapshot_metadata(snap.metadata))
    ds.metadata.version = MEADOW_VERSION

    # create table with metadata from dataframe and underscore all columns
    tb = Table(df, short_name="deaths", underscore=True)

    # add table to a dataset
    ds.add(tb)

    # finally save the dataset
    ds.save()

    log.info("deaths_karlinsky.end")
