"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("patents.csv")

    # Load data from snapshot and adjust columns:
    tb = snap.read(header=3)

    # columns are off by one in original file - we correct this here
    # save metadata to repopulate
    temp_metadata = tb["Office"].metadata.copy()
    temp_cols = tb.columns.copy()
    #fix columns
    tb = tb.drop(columns=["2023"]).reset_index()
    tb.columns = temp_cols

    # repopulate metadata
    for col in tb.columns:
        tb[col].metadata = temp_metadata



    #
    # Process data.
    #
    # Improve tables format.
    tables = [tb.format(["Office", "Field of technology"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
