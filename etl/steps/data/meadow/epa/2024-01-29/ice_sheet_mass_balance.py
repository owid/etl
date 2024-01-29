"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load data from snapshots.
    tb = paths.load_snapshot("ice_sheet_mass_balance.csv").read(skiprows=6)

    #
    # Process data.
    #
    # Ensure all columns are snake-case.
    tb = tb.underscore()

    # Set an appropriate index and sort conveniently.
    # NOTE: There are multiple rows for the same year. This will be fixed in the garden step.
    tb = tb.set_index(["year"], verify_integrity=False).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_meadow.save()
