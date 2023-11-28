"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("lancet_2022.csv")

    # Load data from snapshot.
    tb = snap.read()

    # Remove all characters after "(" in statin use (confidence boundaries for statin use)
    tb["statin_use_secondary"] = tb["statin_use_secondary"].str.split("(").str[0].str.strip().astype(float)
    tb["statin_use_primary"] = tb["statin_use_primary"].str.split("(").str[0].str.strip().astype(float)
    # Drop rows that contain only NaN values and row with Grenadines (actually belongs to St. Vincent and the Grenadines, row above)
    tb = tb[tb["country"] != "Grenadines"]
    tb = tb.dropna(how="all")
    tb["year"] = 2019

    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.underscore().set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
