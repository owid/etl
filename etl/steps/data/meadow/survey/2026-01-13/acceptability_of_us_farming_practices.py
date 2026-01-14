"""Load a snapshot and create a meadow dataset."""

import warnings

from etl.helpers import PathFinder

# Suppress POSIXct warnings from rdata library.
warnings.filterwarnings("ignore", message="Missing constructor for R class")

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Column names of responses.
COLUMNS = [
    "pigs_1",
    "pigs_2",
    "pigs_3",
    "layinghens_1",
    "layinghens_2",
    "layinghens_3",
    "cows_1",
    "cows_2",
    "cows_3",
    "broilers_1",
    "broilers_2",
    "broilers_3",
]


def sanity_check_missing_values(tb):
    # There were missing responses in the RData file.
    # They seem to coincide with the answers "Very acceptable".
    import pandas as pd

    df = pd.read_csv("~/Downloads/data_cleaned.csv")
    for c in COLUMNS:
        assert len(df[df[c] == "Very acceptable"]) == len(tb[tb[c].isnull()]), f"Failed on {c}"


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("acceptability_of_us_farming_practices.rdata")

    # Load data from snapshot.
    tb = snap.read_rda()

    # Looking at their "data_cleaned.csv" file, I see that, for some reason, "Very acceptable" is loaded as nan.
    # Uncomment to confirm this hypothesis; the csv file can be manually downloaded from:
    # https://osf.io/vx5jz/files/yz79e
    # sanity_check_missing_values(tb=tb)

    # So I will fill nans with "Very acceptable".
    for column in COLUMNS:
        tb[column] = tb[column].astype("string").fillna("Very acceptable")

    # Improve table format.
    tb = tb.format(["response_id"])

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb])

    # Save meadow dataset.
    ds_meadow.save()
