"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns to keep and how to rename them.
COLUMNS = {
    "Launch Vehicle ": "vehicle",
    "First Successful Launch ": "first_launch_year",
    "FY21 Launch Cost Per Kilogram ($/kg)": "cost_per_kg",
    "Launch Class ": "launch_class",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("cost_of_space_launches.csv")

    # Read data from snapshot.
    tb = snap.read()

    #
    # Process data.
    #
    # Select and rename columns.
    # NOTE: This could be done in the garden step, but some of the columns raise problems here (possibly due to symbols in the name), so we will just load the ones that are used.
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Improve tables format.
    tables = [tb.format(["vehicle"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
