"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define variables to keep
VARS_TO_KEEP = ["Reference area", "TIME_PERIOD", "Measure", "Poverty line", "Age", "OBS_VALUE"]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("income_distribution_database.csv")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    # Keep only the variables we are interested in.
    tb = tb[VARS_TO_KEEP]

    # Rename "Reference area" to "country", "TIME_PERIOD" to "year" and "OBS_VALUE" to "value".
    tb = tb.rename(columns={"Reference area": "country", "TIME_PERIOD": "year", "OBS_VALUE": "value"})

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year", "measure", "poverty_line", "age"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
