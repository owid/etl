"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    tb = paths.load_snapshot("in_ovo_sexing_market_penetration.csv").read()

    #
    # Process data.
    #
    # Improve table format.
    tb = tb.format(keys=["country", "date", "metric"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb])

    # Save changes in the new meadow dataset.
    ds_meadow.save()
