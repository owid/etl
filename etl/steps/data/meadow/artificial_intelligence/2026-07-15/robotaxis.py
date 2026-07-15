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

    numeric_cols = ["TotalTrips", "TotalPassengersCarried", "TotalPMT"]

    if "Year" in tb.columns and "Month" in tb.columns:
        # Convert columns to numeric, first removing commas from thousands separators
        for col in numeric_cols:
            if col in tb.columns:
                tb[col] = tb[col].astype(str).str.replace(",", "")
                tb[col] = tb[col].replace("", np.nan)
                tb[col] = pd.to_numeric(tb[col], errors="coerce")
                tb[col].metadata.origins = tb["Year"].metadata.origins
        # Drop rows that are completely identical across all columns
        tb = tb.drop_duplicates()
        # Create date column from year and month (using last day of month)
        tb["date"] = pd.to_datetime(tb[["Year", "Month"]].assign(day=1)) + pd.offsets.MonthEnd(1)
        tb = tb.drop(columns=["Year", "Month"])

    # Keep per-carrier rows so values can be verified before aggregation in garden
    tb = tb[["carrier_name", "date"] + numeric_cols]
    # Improve tables format.
    tables = [tb.format(["carrier_name", "date"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, check_variables_metadata=True, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
