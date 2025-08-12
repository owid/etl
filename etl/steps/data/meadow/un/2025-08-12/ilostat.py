"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("ilostat.parquet")

    # Load data from snapshot.
    tb = snap.read()

    print(tb)

    #
    # Process data.
    #
    # Improve tables format.
    tables = [tb.format(["ref_area", "source", "indicator", "sex", "classif1", "classif2", "time"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
