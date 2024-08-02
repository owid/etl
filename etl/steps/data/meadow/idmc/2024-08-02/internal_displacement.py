"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

COLUMNS_TO_DROP = [
    "ISO3",
    "Conflict Stock Displacement (Raw)",
    "Conflict Internal Displacements (Raw)",
    "Disaster Internal Displacements (Raw)",
    "Disaster Stock Displacement (Raw)",
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("internal_displacement.xlsx")

    # Load data from snapshot.
    tb = snap.read()

    # rename and drop columns
    tb = tb.rename(columns={"Name": "country", "Year": "year"})
    tb = tb.drop(columns=COLUMNS_TO_DROP)

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
