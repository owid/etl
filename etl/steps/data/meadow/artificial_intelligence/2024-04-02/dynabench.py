"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("dynabench.xlsx")

    # Load data from snapshot.
    column_names = ["benchmark", "date", "performance"]

    tb = snap.read(sheet_name="Chart Data", header=None, names=column_names)

    tb["date"] = tb["date"].astype(str)  # Convert to string for extracting year
    tb["date"] = tb["date"].str[:4]  # Extract year from date

    tb = tb.rename(columns={"date": "year"})

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently. Note index can be non-unique here.
    tb = tb.underscore().set_index(["benchmark", "year"]).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
