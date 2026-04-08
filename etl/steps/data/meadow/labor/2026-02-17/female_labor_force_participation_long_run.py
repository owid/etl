"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("female_labor_force_participation_long_run.csv")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    #
    # Remove duplicates and keep the last value
    # NOTE: In 1939, there are two values for Germany: one with post WWI borders and one with West Germany borders (without Berlin). We keep the latter, which follows the convention of the rest of the dataset.
    tb = tb.drop_duplicates(subset=["country", "year"], keep="last")

    # Improve tables format.
    tables = [tb.format(["country", "year"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
