"""Load a snapshot and create a meadow dataset."""

import numpy as np

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("epoch_compute_intensive.csv")

    # Load data from snapshot.
    tb = snap.read()

    tb = tb.rename(columns={"Country (of organization)": "Country (from Organization)"})

    #
    # Process data.
    #
    # Define columns of interest.
    cols = [
        "Model",
        "Domain",
        "Authors",
        "Country (from Organization)",
        "Organization",
        "Publication date",
        "Parameters",
        "Training compute (FLOP)",
    ]

    # Check that the columns of interest are present
    for col in cols:
        assert col in tb.columns, f"Column '{col}' is missing from the dataframe."

    # Select the columns of interest
    tb = tb[cols]
    # Replace empty strings with NaN values
    tb = tb.replace("", np.nan)
    # Remove rows where all values are NaN
    tb = tb.dropna(how="all")

    # Convert the training compute column to float
    tb["Training compute (FLOP)"] = tb["Training compute (FLOP)"].astype(float)

    # Replace the missing values in the system column with the organization column. If organization column is NaN as well replace the missing values in the system column with the authors column
    tb["Model"] = tb["Model"].fillna(tb["Organization"]).fillna(tb["Authors"])
    # Check that there are no NaN values in the system column
    assert not tb["Model"].isna().any(), "NaN values found in 'Model' column after processing."
    #
    # Create a new table.
    #
    tb = tb.format(["model", "publication_date"])
    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
