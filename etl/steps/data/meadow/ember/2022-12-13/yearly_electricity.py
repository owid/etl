"""Load snapshot of Ember's Yearly Electricity Data and create a raw data table.

"""
import pandas as pd
from owid.catalog import Dataset, Table

from etl.helpers import Names
from etl.snapshot import Snapshot
from etl.steps.data.converters import convert_snapshot_metadata

# Define snapshot and meadow dataset versions.
SNAPSHOT_VERSION = "2022-12-13"
MEADOW_VERSION = SNAPSHOT_VERSION

# Get naming conventions.
N = Names(__file__)


def run(dest_dir: str) -> None:
    # Retrieve snapshot.
    snap = Snapshot(f"ember/{SNAPSHOT_VERSION}/yearly_electricity.csv")
    df = pd.read_csv(snap.path)

    # Create new dataset and reuse original metadata.
    ds = Dataset.create_empty(dest_dir, metadata=convert_snapshot_metadata(snap.metadata))
    ds.metadata.version = MEADOW_VERSION

    # Create a table with metadata and ensure all columns are snake-case.
    tb = Table(df, short_name=snap.metadata.short_name, underscore=True)

    # Set appropriate indexes.
    tb = tb.set_index(["area", "year", "variable", "unit"], verify_integrity=True)

    # Add table to the new dataset, and save dataset.
    ds.add(tb)
    ds.save()
