"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define columns and their new names
COLUMNS = {"REF_DATE": "year", "GEO": "country", "VALUE": "headcount_ratio_50_median"}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("relative_poverty.csv")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    #
    # Keep only the columns of interest and rename them.
    tb = tb[COLUMNS.keys()].rename(columns=COLUMNS)

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
