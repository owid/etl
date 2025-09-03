"""Load a snapshot and create a meadow dataset."""

import numpy as np
import pandas as pd

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("robotaxis.csv")

    # Load data from snapshot.
    tb = snap.read(low_memory=False)
    #
    # Process data.
    #

    # Sum across all TCPID for each year and month
    numeric_cols = ["TotalTrips", "TotalPassengersCarried", "TotalPMT"]

    if "Year" in tb.columns and "Month" in tb.columns:
        # Convert columns to numeric, first removing commas from thousands separators
        for col in numeric_cols:
            if col in tb.columns:
                # Remove commas and convert to numeric
                tb[col] = tb[col].astype(str).str.replace(",", "")
                tb[col] = tb[col].replace("", np.nan)
                tb[col] = pd.to_numeric(tb[col], errors="coerce")
                tb[col].metadata.origins = tb["Year"].metadata.origins
        # Drop rows that are *completely identical* across all columns
        tb = tb.drop_duplicates()
        # Group by Year and Month, sum the numeric columns
        tb = tb.groupby(["Year", "Month"])[numeric_cols].sum().reset_index()
        # Create date column from year and month (using first day of month)
        tb["date"] = pd.to_datetime(tb[["Year", "Month"]].assign(day=1)) + pd.offsets.MonthEnd(1)

        # Drop individual Year and Month columns
        tb = tb.drop(columns=["Year", "Month"])

    # Improve tables format.
    tables = [tb.format(["date"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, check_variables_metadata=True, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
