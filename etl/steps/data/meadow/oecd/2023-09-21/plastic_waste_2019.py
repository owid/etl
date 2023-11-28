"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.

    snap = paths.load_snapshot("plastic_waste_2019.csv")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    #
    columns_to_use = ["Location", "Plastics polymer", "Plastics applications", "Value"]
    tb = tb[columns_to_use]

    rename_cols = {
        "Location": "country",
        "Plastics polymer": "polymer",
        "Plastics applications": "application",
        "Value": "plastic_waste",
    }

    tb = tb.rename(columns=rename_cols)
    tb["year"] = 2019

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.underscore().set_index(["country", "year", "polymer", "application"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
