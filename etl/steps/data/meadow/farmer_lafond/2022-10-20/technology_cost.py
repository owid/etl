import pandas as pd
from owid.catalog import Dataset, Table, TableMeta
from owid.catalog.utils import underscore_table

from etl.helpers import Names
from etl.snapshot import Snapshot
from etl.steps.data.converters import convert_snapshot_metadata

# Walden dataset version.
WALDEN_VERSION = "2022-10-20"
VERSION = WALDEN_VERSION
# Get naming conventions.
N = Names(__file__)


def run(dest_dir: str) -> None:
    # Load raw data from Walden.
    snap = Snapshot(f"farmer_lafond/{WALDEN_VERSION}/technology_cost.csv")
    local_file = str(snap.path)
    df = pd.read_csv(local_file)

    # Set an appropriate index and sort conveniently.
    df = df.set_index(["item", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    # Create new dataset and reuse Walden metadata.
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_snapshot_metadata(snap.metadata)
    ds.metadata.version = VERSION

    # Create new table with metadata from dataframe.
    table_metadata = TableMeta(
        short_name=snap.metadata.short_name,
        title=snap.metadata.name,
        description=snap.metadata.description,
    )
    tb = Table(df, metadata=table_metadata)

    # Underscore all table columns.
    tb = underscore_table(tb)

    # Add table to new Meadow dataset and save dataset.
    ds.add(tb)
    ds.save()
