"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("space_track.csv")

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
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save garden dataset.
    ds_meadow.save()
