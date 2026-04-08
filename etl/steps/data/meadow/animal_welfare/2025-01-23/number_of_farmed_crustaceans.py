"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load and process inputs.
    #
    # Read snapshot.
    snap = paths.load_snapshot("number_of_farmed_crustaceans.csv")
    tb = snap.read()

    # Set an appropriate index and sort conveniently.
    tb = tb.format(["country", "year"], verify_integrity=True, sort_columns=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb], check_variables_metadata=True)
    ds_meadow.save()
