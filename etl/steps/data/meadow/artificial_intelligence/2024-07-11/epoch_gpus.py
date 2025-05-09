"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("epoch_gpus.csv")

    # Load data from snapshot.
    tb = snap.read(safe_types=False)
    #
    # Process data.
    #
    # Define columns of interest.
    cols = [
        "Manufacturer",
        "Name of the hardware",
        "FP32 (single precision) Performance (FLOP/s)",
        "Release date",
        "Release price (USD)",
    ]

    # Check that the columns of interest are present
    for col in cols:
        assert col in tb.columns, f"Column '{col}' is missing from the dataframe."

    # Select the columns of interest
    tb = tb[cols]

    # Check that there are no NaN values in the name of the hardware column
    assert not tb["Name of the hardware"].isna().any(), "NaN values found in 'Name of the hardware' column."
    #
    # Create a new table and ensure all columns are snake-case.
    #
    tb = tb.format(["name_of_the_hardware", "release_date"])

    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)
    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
