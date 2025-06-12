"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("objects_in_space.csv")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    #
    # Improve table format.
    tb = tb.format(["gp_id"])

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save garden dataset.
    ds_meadow.save()
