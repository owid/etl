"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("gpei_funding.xlsx")

    # Load data from snapshot.
    tb = snap.read(skiprows=2)
    tb_orig = tb
    #
    # Process data.
    #
    tb = tb.pivot_table(columns="Historicals Classif").reset_index().rename(columns={"index": "year"})
    tb["country"] = "World"
    for col in tb.columns:
        if col != "year" and col != "country":
            # Hacky way to ensure that the metadata is copied over.
            tb[col] = tb[col].copy_metadata(tb_orig["Historicals Classif"])
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
