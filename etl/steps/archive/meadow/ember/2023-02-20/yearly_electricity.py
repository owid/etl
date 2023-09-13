"""Load snapshot of Ember's Yearly Electricity Data and create a raw data table.

"""
import pandas as pd
from owid.catalog import Dataset, Table

from etl.helpers import PathFinder
from etl.steps.data.converters import convert_snapshot_metadata

# Get naming conventions.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # Retrieve snapshot.
    snap = paths.load_dependency("yearly_electricity.csv")
    df = pd.read_csv(snap.path)

    # Create new dataset and reuse original metadata.
    ds = Dataset.create_empty(dest_dir, metadata=convert_snapshot_metadata(snap.metadata))
    ds.metadata.version = paths.version

    # Create a table with metadata and ensure all columns are snake-case.
    tb = Table(df, short_name=snap.metadata.short_name, underscore=True)

    # Set appropriate indexes.
    tb = tb.set_index(["area", "year", "variable", "unit"], verify_integrity=True)

    # Add table to the new dataset, and save dataset.
    ds.add(tb)
    ds.save()
