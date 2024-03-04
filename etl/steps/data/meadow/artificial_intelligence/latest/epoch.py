"""Load a snapshot and create a meadow dataset."""

import numpy as np
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("epoch.start")

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
        "System",
        "Domain",
        "Organization",
        "Organization categorization",
        "Publication date",
        "Parameters",
        "Training compute (FLOP)",
        "Training dataset size (datapoints)",
        "Training time (hours)",
        "Notability criteria",
        "Approach",
    ]

    # Check that the columns of interest are present
    for col in cols:
        assert col in tb.columns, f"Column '{col}' is missing from the dataframe."

    # Select the columns of interest
    tb = tb[cols]
    # Replace empty strings with NaN values
    tb.replace("", np.nan, inplace=True)
    # Remove rows where all values are NaN
    tb = tb.dropna(how="all")

    # Convert the training compute column to float
    tb["Training compute (FLOP)"] = tb["Training compute (FLOP)"].astype(float)

    # Replace the missing values in the system column with the organization column
    tb.loc[tb["System"].isna(), "System"] = tb.loc[tb["System"].isna(), "Organization"]

    # Check that there are no NaN values in the system column
    assert not tb["System"].isna().any(), "NaN values found in 'System' column after processing."
    #
    # Create a new table and ensure all columns are snake-case.
    #
    tb = tb.underscore().set_index(["system", "publication_date"], verify_integrity=True).sort_index()

    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("epoch.end")
