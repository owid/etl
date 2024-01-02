"""Load snapshot of Farmer & Lafond (2016) data and create a table.

"""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current data step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load snapshot.
    snap = paths.load_snapshot("farmer_lafond_2016.csv")
    tb = snap.read()

    #
    # Prepare data.
    #
    # The zeroth row will be added as metadata, and the first row is not useful, so drop both.
    tb = tb.drop(index=[0, 1]).reset_index(drop=True)

    # Rename year column and make it integer.
    tb = tb.rename(columns={"YEAR": "year"}).astype({"year": int}, errors="raise")

    # Ensure all columns are snake-case, set an appropriate index and sort conveniently.
    tb = tb.underscore().set_index(["year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset and reuse snapshot metadata.
    ds = create_dataset(dest_dir=dest_dir, tables=[tb], check_variables_metadata=True)
    ds.save()
