"""Load a snapshot and create a meadow dataset."""

import pandas as pd

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("epoch_price_reduction.csv")

    # Read snapshot
    tb = snap.read()

    #
    # Process data.
    #
    #
    # Create a new table.
    #
    tb = tb[["bench", "threshold_model", "end_date", "price_reduction_factor_per_year"]]
    tb["year"] = 2025
    tb["price_reduction_factor_per_year"] = tb["price_reduction_factor_per_year"].astype(float)
    tb = tb.drop(columns=["end_date"])
    tb = tb.format(["bench", "threshold_model", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    paths.log.info("epoch.end")
