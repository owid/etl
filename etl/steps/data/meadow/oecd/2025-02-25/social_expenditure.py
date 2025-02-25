"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define columns to keep and their new names
COLUMNS_TO_KEEP = {
    "Reference area": "country",
    "TIME_PERIOD": "year",
    "Unit of measure": "indicator",
    "Expenditure source": "expenditure_source",
    "Spending type": "spending_type",
    "Programme type": "programme_type",
    "OBS_VALUE": "value",
    "Observation status": "status",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("social_expenditure.csv")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    #
    # Keep only the columns of interest.
    tb = tb[COLUMNS_TO_KEEP.keys()]

    # Rename columns.
    tb = tb.rename(columns=COLUMNS_TO_KEEP)

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tables = [tb.format(["country", "year"])]

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir,
        tables=tables,
        check_variables_metadata=True,
        default_metadata=snap.metadata,
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()
