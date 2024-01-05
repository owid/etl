"""Load snapshot of Ember's Yearly Electricity Data and create a raw data table.

"""
from etl.helpers import PathFinder, create_dataset

# Get naming conventions.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("yearly_electricity.csv")
    tb = snap.read(underscore=True)

    #
    # Process data.
    #
    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["area", "year", "variable", "unit"], verify_integrity=True).sort_index().sort_index(axis=1)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_meadow.save()
