"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("status_of_world_nuclear_forces.csv")

    # Load data from snapshot.
    tb = snap.read(na_values="n.a.")

    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index and sort conveniently.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb])
    ds_meadow.save()
