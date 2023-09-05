"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("dummy.csv")

    # Load data from snapshot.
    tb = snap.read()

    tb["dummy_variable"] += 3

    tb["yummy_variable"] = tb["dummy_variable"] * 2

    # tb2 = tb.copy().rename(columns={"dummy_variable": "yummy_variable"})
    # tb2["yummy_variable"] += 1

    # tb["yummy_variable"] = tb["dummy_variable"].copy()

    # tb["yummy_variable"] += 1

    tb["out"] = tb["dummy_variable"] + tb["yummy_variable"]

    # print(tb.dummy_variable.metadata.processing_log)
    # print("pp tb.dummy_variable.metadata.processing_log")

    # tb["out"] = tb["dummy_variable"] + tb["yummy_variable"]

    #
    # Process data.
    #
    # Create a new table and ensure all columns are snake-case.
    tb = tb.underscore()

    tb = tb.set_index(["country", "year"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()
