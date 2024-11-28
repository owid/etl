"""Load a snapshot and create a meadow dataset."""

from shared import clean_data

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("gbd_mental_health.feather")

    # Load data from snapshot.
    tb = snap.read(safe_types=False)

    # standardize column names
    tb = clean_data(tb)
    #
    # Process data.
    #
    # Drop the sex column if it is not needed
    if all(tb["sex"] == "Both"):
        tb = tb.drop(columns="sex")
    if all(tb["measure"] == "Prevalence"):
        tb = tb.drop(columns="measure")
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    cols = tb.columns.drop("value").to_list()
    tb = tb.format(cols)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
