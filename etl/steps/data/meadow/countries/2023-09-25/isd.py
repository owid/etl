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
    snap = paths.load_snapshot("isd.xlsx")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.underscore().set_index(["cownum", "start", "end"], verify_integrity=True).sort_index()

    # Dtypes
    tb = tb.astype({"othername": str, "decdate": str, "latitude": str, "longitude": str})

    # Replace -9 -> NaN
    tb = tb.replace(
        {
            -9: np.nan,
            "-9": np.nan,
        }
    )

    # Remove -9 from categorical columns
    columns_cat = tb.dtypes[tb.dtypes.isin(["category", "object"])].index
    tb[columns_cat] = tb[columns_cat].astype("category")

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
