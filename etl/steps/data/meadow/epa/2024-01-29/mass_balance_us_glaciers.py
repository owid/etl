"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
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
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_meadow.save()
