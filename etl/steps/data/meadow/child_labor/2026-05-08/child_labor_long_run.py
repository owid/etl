"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    snapshot_names = [
        "child_labor_us_long.csv",
        "child_labor_belgium.csv",
        "child_labor_portugal.csv",
        "child_labor_denmark.csv",
        "child_labor_italy.csv",
        "child_labor_sweden.csv",
        "child_labor_us_carter_sutch.csv",
        "child_labor_portugal_goulart_bedi.csv",
        "child_labor_england_wales_scotland.csv",
        "child_labor_england_wales.csv",
        "child_labor_japan.csv",
    ]
    tables = []
    for snapshot_name in snapshot_names:
        # Retrieve snapshot.
        snap = paths.load_snapshot(snapshot_name)

        # Load data from snapshot.
        tb = snap.read()

        #
        # Process data.
        #
        # Improve table format.
        tb = tb.format(["country", "year"])

        # Append current table to list of tables.
        tables.append(tb)

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables)

    # Save meadow dataset.
    ds_meadow.save()
