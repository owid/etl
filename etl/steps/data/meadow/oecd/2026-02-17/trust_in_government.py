"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define columns to keep and their new names
COLUMNS_TO_KEEP = {
    "Reference area": "country",
    "TIME_PERIOD": "year",
    "OBS_VALUE": "trust_in_government",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("trust_in_government.csv")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    #
    # Select only columns of interest and rename them.
    tb = tb[list(COLUMNS_TO_KEEP.keys())].rename(columns=COLUMNS_TO_KEEP, errors="raise")

    # Improve tables format.
    tables = [tb.format(["country", "year"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
