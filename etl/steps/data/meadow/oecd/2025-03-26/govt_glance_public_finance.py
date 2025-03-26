"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define columns to keep and their new names
COLUMNS_TO_KEEP = {
    "Reference area": "country",
    "TIME_PERIOD": "year",
    "Measure": "indicator",
    "Unit of measure": "unit",
    "OBS_VALUE": "value",
    "Observation status": "status",
    "Unit multiplier": "unit_multiplier",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("govt_glance_public_finance.csv")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    #
    # Keep only the columns of interest.
    tb = tb[COLUMNS_TO_KEEP.keys()]

    # Rename columns.
    tb = tb.rename(columns=COLUMNS_TO_KEEP)

    # Drop status column.
    tb = tb.drop(columns=["status"])

    # Improve tables format.
    tables = [tb.format(["country", "year", "indicator", "unit"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
