"""Load Stack Overflow AI usage snapshot into meadow."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    """Create meadow dataset."""
    #
    # Load inputs.
    #
    snap = paths.load_snapshot("stackoverflow_ai_usage.csv")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    #
    tb = tb.format(["year", "response"])

    #
    # Save outputs.
    #
    ds_meadow = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)
    ds_meadow.save()
