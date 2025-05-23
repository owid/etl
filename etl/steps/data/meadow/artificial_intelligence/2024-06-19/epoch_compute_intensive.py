"""Load a snapshot and create a meadow dataset."""

import numpy as np

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("epoch_compute_intensive.csv")

    # Load data from snapshot.
    tb = snap.read(safe_types=False)

    #
    # Process data.
    #
    # Define columns of interest.
    cols = [
        "System",
        "Domain",
        "Authors",
        "Country (from Organization)",
        "Organization",
        "Organization categorization",
        "Publication date",
        "Parameters",
        "Training compute (FLOP)",
        "Training dataset size (datapoints)",
        "Notability criteria",
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
    tb["System"] = tb["System"].fillna(tb["Organization"]).fillna(tb["Authors"])
    # Check that there are no NaN values in the system column
    assert not tb["System"].isna().any(), "NaN values found in 'System' column after processing."
    #
    # Create a new table.
    #
    tb = tb.format(["system", "publication_date"])
    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
