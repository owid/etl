"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("smil_2017.csv")

    # Load data from snapshot.
    tb = snap.read(underscore=False)

    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index and sort conveniently.
    tb = tb.format(sort_columns=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb])
    ds_meadow.save()
