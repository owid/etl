"""Load a snapshot and create a meadow dataset."""

import numpy as np

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    paths.log.info("epoch.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("epoch.csv")

    # Read snapshot
    tb = snap.read()

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
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    paths.log.info("epoch.end")
