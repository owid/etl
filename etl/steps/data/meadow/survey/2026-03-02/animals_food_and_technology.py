"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("animals_food_and_technology.csv")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    #
    # Given that there is no identifier, create a per-year dummy index.
    tb["entry_id"] = tb.groupby("year").cumcount()

    # Improve table format.
    tb = tb.format(["entry_id", "year"])

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
