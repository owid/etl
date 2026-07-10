"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("future_of_food_and_agriculture_regions.csv")

    # Load data from snapshot.
    tb = snap.read_csv()

    #
    # Process data.
    #
    # Improve table format.
    tb = tb.format(["domain", "indicator", "item", "element", "region", "scenario", "year"])

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
