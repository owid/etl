"""Load a snapshot and create a meadow dataset."""

import pandas as pd

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("corn_yields.xls")

    # Load data from snapshot.
    # Note that, despite USDA/FAS generating files with ".xls" extension, they are HTML files.
    tb = snap.read_custom(read_function=lambda x: pd.read_html(x)[0])

    #
    # Process data.
    #
    # Improve tables format.
    tables = [tb.format(["country"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
