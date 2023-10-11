"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("net_zero_tracker.xlsx")

    # Load data from snapshot.
    tb = snap.read(underscore=True)

    #
    # Process data.
    #
    # Fix wrong column types.
    for column in ["end_target_text", "interim_target_intensity_unit"]:
        tb[column] = tb[column].fillna("").astype(str)

    # Set an appropriate index and sort conveniently.
    # NOTE: There are multiple rows for the same country-year (this will be handled in the garden step).
    tb = tb.set_index(["id_code"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
