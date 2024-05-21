"""Load a snapshot and create a meadow dataset."""

from shared import clean_data, fix_percent

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("gbd_cause.csv")

    # Load data from snapshot.
    tb = snap.read()
    # standardize column names
    tb = clean_data(tb)
    # fix percent values - they aren't consistently presented as either 0-1, or 0-100.
    tb = fix_percent(tb)
    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    if all(tb["sex"] == "Both"):
        tb = tb.drop(columns="sex")
    tb = tb.format(["country", "year", "measure", "age", "cause", "metric"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
