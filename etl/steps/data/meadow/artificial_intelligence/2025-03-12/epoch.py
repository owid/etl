"""Load a snapshot and create a meadow dataset."""

import numpy as np
import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    paths.log.info("epoch.start")

    #
    # Load inputs.
    #
    # Retrieve snapshots.
    snap = paths.load_snapshot("epoch.csv")
    snap_all = paths.load_snapshot("epoch_all_ai_models.csv")

    # Read snapshots.
    tb = snap.read()
    tb_all = snap_all.read()

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
        "Organization categorization",
        "Publication date",
        "Parameters",
        "Training compute (FLOP)",
        "Training dataset size (total)",
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

    # Add how training compute was found, which only the file of all models has.
    tb = add_training_compute_estimation_method(tb=tb, tb_all=tb_all)

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
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    paths.log.info("epoch.end")


def add_training_compute_estimation_method(tb, tb_all):
    """Add Epoch's training compute estimation method to the notable models, matched by model name."""
    column = "Training compute estimation method"
    assert column in tb_all.columns, f"Column '{column}' is missing from the file of all models."
    tb_all = tb_all[tb_all["Model"].isin(tb["Model"])][["Model", column]]

    # Every notable model should be in the file of all models, once.
    missing = set(tb["Model"]) - set(tb_all["Model"])
    assert not missing, f"Notable models missing from the file of all models: {sorted(missing)}"
    duplicated = tb_all[tb_all["Model"].duplicated()]["Model"].tolist()
    assert not duplicated, f"Notable models listed more than once in the file of all models: {duplicated}"

    tb = pr.merge(tb, tb_all, on="Model", how="left")

    return tb
