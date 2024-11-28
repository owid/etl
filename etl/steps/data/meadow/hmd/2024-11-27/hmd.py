"""Load a snapshot and create a meadow dataset."""

import os

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


TABLES_LIFE_TABLES = [
    "lt_male",
    "lt_female",
    "lt_both",
    "c_lt_male",
    "c_lt_female",
    "c_lt_both",
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("hmd.zip")

    # Load data from snapshot.
    with snap.extract_to_tempdir() as tmpdir:
        print(os.listdir(tmpdir))
    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
