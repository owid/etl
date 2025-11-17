"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    """Create meadow dataset."""
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("nvidia_revenue.csv")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    #

    # Set index
    tb = tb.format(["date", "segment"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)
    # Save changes in the new meadow dataset.
    ds_meadow.save()
