"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("gfs_wave_one.csv")

    # Load data from snapshot.
    tb = snap.read()

    # fix dtypes for object columns
    for col in tb.columns:
        if tb[col].dtype == "object":
            tb[col] = tb[col].astype("str")

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["id"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
