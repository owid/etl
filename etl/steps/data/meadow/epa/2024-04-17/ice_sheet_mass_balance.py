"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load data from snapshots.
    tb = paths.load_snapshot("ice_sheet_mass_balance.csv").read(skiprows=6)

    #
    # Process data.
    #
    # Improve table format.
    # NOTE: There are multiple rows for the same year. This will be fixed in the garden step.
    tb = tb.format(["year"], verify_integrity=False)

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb])
    ds_meadow.save()
