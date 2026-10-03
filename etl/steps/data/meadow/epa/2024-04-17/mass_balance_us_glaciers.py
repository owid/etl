"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load data from snapshots.
    tb = paths.load_snapshot("mass_balance_us_glaciers.csv").read(skiprows=6)

    #
    # Process data.
    #
    # Set an appropriate index and sort conveniently.
    tb = tb.format(["year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb])
    ds_meadow.save()
