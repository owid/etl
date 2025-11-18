"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
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
    # Define columns of interest - selecting key columns for GPU hardware analysis
    cols = [
        "Hardware name",
        "Manufacturer",
        "Type",
        "Release date",
        "Release price (USD)",
        "FP32 (single precision) performance (FLOP/s)",
        "FP16 (half precision) performance (FLOP/s)",
        "INT8 performance (OP/s)",
        "Memory size per board (Byte)",
        "Memory bandwidth (byte/s)",
        "TDP (W)",
        "Process size (nm)",
    ]

    # Check that the columns of interest are present
    for col in cols:
        assert col in tb.columns, f"Column '{col}' is missing from the dataframe."

    # Select the columns of interest
    tb = tb[cols]

    # Check that there are no NaN values in the hardware name column
    assert not tb["Hardware name"].isna().any(), "NaN values found in 'Hardware name' column."
    #
    # Create a new table and ensure all columns are snake-case.
    #
    tb = tb.format(["hardware_name", "release_date"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
