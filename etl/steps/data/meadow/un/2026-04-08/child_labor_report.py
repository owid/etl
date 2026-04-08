"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    snapshot_names = ["child_labor_trends.csv", "child_labor_by_region.csv", "hazardous_work_by_region.csv"]
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
