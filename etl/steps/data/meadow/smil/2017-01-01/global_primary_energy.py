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

# Version of meadow dataset to be created.
VERSION = "2017-01-01"
# Walden version of the dataset.
WALDEN_VERSION = "2017-01-01"


def run(dest_dir: str) -> None:
    log.info("global_primary_energy.start")

    # Retrieve raw data from walden.
    snap = Snapshot(f"smil/{WALDEN_VERSION}/global_primary_energy.csv")
    local_file = str(snap.path)
    df = pd.read_csv(local_file)

    # Create a new meadow dataset and reuse walden metadata.
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_snapshot_metadata(snap.metadata)
    ds.metadata.version = VERSION

    # Create a new table with metadata from the dataset.
    table_metadata = TableMeta(
        short_name=snap.metadata.short_name, title=snap.metadata.name, description=snap.metadata.description
    )
    tb = Table(df, metadata=table_metadata)
    # Use the current names of the columns as the variable titles in the metadata.
    for column in tb.columns:
        tb[column].metadata.title = column
    # Change all columns to be lower snake case.
    tb = underscore_table(tb)
    # Set table index.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index()

    # Add table to dataset.
    ds.add(tb)

    # Save dataset.
    ds.save()

    log.info("global_primary_energy.end")
