"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    tb = paths.load_snapshot("chick_culling_laws.csv").read()

    #
    # Process data.
    #
    # Improve table format.
    tb = tb.format(keys=["country"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb])

    # Save changes in the new meadow dataset.
    ds_meadow.save()
