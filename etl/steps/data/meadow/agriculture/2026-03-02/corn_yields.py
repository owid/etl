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
    # Also, note that the online tool fills empty data points with zeros when more than one country is fetched.
    # For example, if one downloads only corn yields for Romania, the data ends in 1999; however, if one downloads corn yields for Romania and Russia, the resulting table contains zeros for Romania from 2000 onwards.
    # I couldn't find any straightforward option to avoid these spurious zeros; they will be handled in the garden step.
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
