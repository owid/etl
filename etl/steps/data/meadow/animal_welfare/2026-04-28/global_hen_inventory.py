"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("global_hen_inventory.csv")

    # Load data from snapshot.
    tb = snap.read(sep="\t", encoding="utf-16", engine="python")

    #
    # Process data.
    #
    # Improve table format.
    tb = tb.format()

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save the new meadow dataset.
    ds_meadow.save()
