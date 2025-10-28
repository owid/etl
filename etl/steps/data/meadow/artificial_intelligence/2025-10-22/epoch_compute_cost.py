"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot and load data.
    snap = paths.load_snapshot("epoch_compute_cost.csv")
    tb = snap.read(safe_types=False)

    #
    # Process data.
    #
    # Define and validate columns of interest.
    columns_to_keep = [
        "Model",
        "Domain",
        "Publication date",
        "Cost (inflation-adjusted)",
    ]

    # Validate that all required columns are present.
    missing_columns = set(columns_to_keep) - set(tb.columns)
    assert not missing_columns, f"Missing columns: {missing_columns}"

    # Select only the columns of interest.
    tb = tb[columns_to_keep]

    # Validate that Model column has no NaN values.
    assert not tb["Model"].isna().any(), "NaN values found in 'Model' column"

    #
    # Format table and ensure all columns are snake-case.
    #
    tb = tb.format(["model", "publication_date"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
