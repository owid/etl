"""Load a snapshot and create a meadow dataset."""


import numpy as np
from owid.catalog import Table
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
    df = snap.read()
    #
    # Process data.
    #
    # Define columns of interest.
    cols = [
        "System",
        "Domain",
        "Organization",
        "Organization Categorization",
        "Publication date",
        "Parameters",
        "Training compute (FLOP)",
        "Training dataset size (datapoints)",
        "Training time (hours)",
        "Notability criteria",
    ]
    # Check that the columns of interest are present
    for col in cols:
        assert col in df.columns, f"Column '{col}' is missing from the dataframe."
    # Check that there are no negative values in the training compute column
    assert not (df["Training compute (FLOP)"] < 0).any(), "Negative values found in 'Training compute (FLOP)' column."

    # Select the columns of interest
    df = df[cols]
    # Replace empty strings with NaN values
    df.replace("", np.nan, inplace=True)
    # Convert the training compute column to float
    df["Training compute (FLOP)"] = df["Training compute (FLOP)"].astype(float)

    # Replace the missing values in the system column with the organization column
    df.loc[df["System"].isna(), "System"] = df.loc[df["System"].isna(), "Organization"]
    # Check that there are no NaN values in the system column
    assert not df["System"].isna().any(), "NaN values found in 'System' column after processing."
    # Drop the organization column
    df.drop("Organization", axis=1, inplace=True)
    #
    # Create a new table and ensure all columns are snake-case.
    #
    tb = Table(df, short_name=paths.short_name, underscore=True)
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("epoch.end")
