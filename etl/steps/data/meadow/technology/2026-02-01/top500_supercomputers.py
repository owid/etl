"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("top500_supercomputers.csv")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    #
    # Clean column names (convert to snake_case)

    tb = tb[["Rank", "list_year", "list_month", "RMax"]]

    # Improve tables format.
    tables = [tb.format(["rank", "list_year", "list_month"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
