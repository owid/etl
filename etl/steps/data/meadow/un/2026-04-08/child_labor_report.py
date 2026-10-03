"""Load a snapshot and create a meadow dataset."""

import pandas as pd

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

SNAPSHOTS = {
    "child_labor_by_region": {
        "index": ["region_type", "region"],
    },
    "hazardous_work_by_region": {
        "index": ["region_type", "region"],
    },
    "child_labor_trends": {
        "index": ["disaggregation_type", "disaggregation_value"],
    },
}


def run() -> None:
    #
    # Load inputs.
    #
    tables = []
    for snap_name, meta in SNAPSHOTS.items():
        # Retrieve snapshot.
        snap = paths.load_snapshot(f"{snap_name}.csv")

        # Load data from snapshot.
        tb = snap.read()

        # Convert numeric columns from comma-formatted strings to float.
        num_cols = [c for c in tb.columns if c not in meta["index"]]
        tb[num_cols] = tb[num_cols].apply(
            lambda col: pd.to_numeric(col.astype(str).str.replace(",", "", regex=False), errors="coerce")
        )

        #
        # Improve table format.
        #
        tb = tb.format(meta["index"])

        # Append current table to list of tables.
        tables.append(tb)

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables)

    # Save meadow dataset.
    ds_meadow.save()
