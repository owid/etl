"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("plastic_aquatic_leakage.csv")

    # Load data from snapshot.
    tb = snap.read(safe_types=False)
    #
    # Process data.
    #
    columns_to_use = ["Location", "Variable", "Time", "Value"]
    tb = tb[columns_to_use]

    rename_cols = {"Location": "country", "Variable": "leakage_type", "Time": "year"}
    tb = tb.rename(columns=rename_cols)

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.underscore().set_index(["country", "year", "leakage_type"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
