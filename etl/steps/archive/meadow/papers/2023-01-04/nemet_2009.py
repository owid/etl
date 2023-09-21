"""Load snapshot of Nemet (2009) data and create a table.

"""

import pandas as pd
from owid import catalog

from etl.helpers import PathFinder
from etl.steps.data.converters import convert_snapshot_metadata

# Get paths and naming conventions for current data step.
paths = PathFinder(__file__)

# Columns to select from snapshot, and how to rename them.
COLUMNS = {
    "Cost (2004 USD/Watt)": "cost",
    "Time (Year)": "year",
    "Yearly Capacity (MW)": "yearly_capacity",
    "Previous Capacity (MW)": "previous_capacity",
}


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load snapshot.
    snap = paths.load_dependency("nemet_2009.csv")
    df = pd.read_csv(snap.path)

    #
    # Process data.
    #
    df = df.rename(columns=COLUMNS, errors="raise")[COLUMNS.values()]

    # Set an appropriate index and sort conveniently.
    df = df.set_index(["year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset and reuse snapshot metadata.
    ds = catalog.Dataset.create_empty(dest_dir, metadata=convert_snapshot_metadata(snap.metadata))
    ds.metadata.version = "2023-01-04"

    # Create a new table.
    tb = catalog.Table(df, short_name=paths.short_name, underscore=True)

    # Add table to dataset and save dataset.
    ds.add(tb)
    ds.save()
