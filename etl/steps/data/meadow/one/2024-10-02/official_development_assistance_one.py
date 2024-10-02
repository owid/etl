"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define columns to keep
COLUMNS_TO_KEEP = [
    "year",
    "donor_name",
    "recipient_name",
    "sector_name",
    "purpose_name",
    "channel_name",
    "value",
]

# Define index columns (COLUMS_TO_KEEP minus value)
INDEX_COLUMNS = [col for col in COLUMNS_TO_KEEP if col != "value"]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("official_development_assistance_one.feather")

    # Load data from snapshot.
    tb = snap.read()

    print(tb)

    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(INDEX_COLUMNS)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
