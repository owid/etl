"""Load snapshot of Farmer & Lafond (2016) data and create a table.

"""

import pandas as pd
from owid import catalog

from etl.helpers import PathFinder
from etl.steps.data.converters import convert_snapshot_metadata

# Get paths and naming conventions for current data step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load snapshot.
    snap = paths.load_dependency("farmer_lafond_2016.csv")
    df = pd.read_csv(snap.path)

    #
    # Prepare data.
    #
    # Store the unit of each technology cost from the zeroth row.
    units = dict(zip(df.columns.tolist()[1:], df.loc[0][1:]))

    # The zeroth row will be added as metadata, and the first row is not useful, so drop both.
    df = df.drop(index=[0, 1]).reset_index(drop=True)

    # Rename year column and make it integer.
    df = df.rename(columns={"YEAR": "year"}).astype({"year": int})

    # Create a new table with metadata.
    tb = catalog.Table(df, short_name=paths.short_name, underscore=False)

    # Add title, units and description to metadata.
    for column in tb.drop(columns=["year"]).columns:
        tb[column].metadata.title = column
        tb[column].metadata.unit = units[column]
        tb[column].metadata.description = f"Cost for {column}, measured in {units[column]}."

    # Ensure all columns are snake-case.
    tb = catalog.utils.underscore_table(tb)

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset and reuse snapshot metadata.
    ds = catalog.Dataset.create_empty(dest_dir, metadata=convert_snapshot_metadata(snap.metadata))
    ds.metadata.version = "2023-01-04"

    # Add table to dataset and save dataset.
    ds.add(tb)
    ds.save()
