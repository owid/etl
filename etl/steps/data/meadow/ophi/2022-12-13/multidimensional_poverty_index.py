"""Load snapshot of Multidimensional Poverty Index data and prepare a table with basic metadata.
"""

import pandas as pd
from owid.catalog import Dataset, Table, TableMeta
from structlog import get_logger

from etl.helpers import PathFinder
from etl.snapshot import Snapshot
from etl.steps.data.converters import convert_snapshot_metadata

log = get_logger()

# Snapshot version.
SNAPSHOT_VERSION = "2022-12-13"
# Current Meadow dataset version.
MEADOW_VERSION = SNAPSHOT_VERSION

# naming conventions
N = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("multidimensional_poverty_index.start")

    # retrieve snapshot
    snap = Snapshot(f"ophi/{SNAPSHOT_VERSION}/multidimensional_poverty_index.csv")
    df = pd.read_csv(snap.path)

    df = df.rename(columns={"cty_lab": "country"})

    # create new dataset and reuse walden metadata
    ds = Dataset.create_empty(dest_dir, metadata=convert_snapshot_metadata(snap.metadata))
    ds.metadata.version = MEADOW_VERSION

    # # create table with metadata from dataframe

    table_metadata = TableMeta(
        short_name=snap.metadata.short_name,
        title=snap.metadata.name,
        description=snap.metadata.description,
    )
    tb = Table(df, metadata=table_metadata)

    # add table to a dataset
    ds.add(tb)

    # update metadata

    ds.metadata.update_from_yaml(N.metadata_path, if_source_exists="replace")
    tb.update_metadata_from_yaml(N.metadata_path, "multidimensional_poverty_index")

    # finally save the dataset
    ds.save()

    log.info("multidimensional_poverty_index.end")
