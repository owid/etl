"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# Define new names for columns
INDICATOR_NAMES = {
    "Reference area": "country",
    "TIME_PERIOD": "year",
    "Aggregation operation": "indicator",
    "OBS_VALUE": "value",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("decile_ratios.csv")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    # Keep only the variables we are interested in and rename them.
    tb = tb[INDICATOR_NAMES.keys()].rename(columns=INDICATOR_NAMES, errors="raise")

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year", "indicator"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
