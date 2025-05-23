"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("plastic_use_application.csv")

    # Load data from snapshot.
    tb = snap.read(safe_types=False)
    #
    # Process data.
    #
    columns_to_use = ["Plastics applications", "Time", "Value"]
    tb = tb[columns_to_use]
    rename_cols = {"Plastics applications": "application", "Time": "year"}

    tb = tb.rename(columns=rename_cols)
    tb["country"] = "World"

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.underscore().set_index(["country", "year", "application"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
