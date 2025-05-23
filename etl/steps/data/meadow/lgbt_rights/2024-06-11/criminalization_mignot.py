"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define column names
COLUMN_NAMES = [
    "country",
    "last_decriminalization",
    "last_criminalization",
    "former_decriminalizations",
    "former_criminalizations",
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("criminalization_mignot.csv")

    # Load data from snapshot.
    tb = snap.read(safe_types=False, header=None, names=COLUMN_NAMES)

    #
    # Process data.
    #
    # Add column names.
    tb.columns = COLUMN_NAMES

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
