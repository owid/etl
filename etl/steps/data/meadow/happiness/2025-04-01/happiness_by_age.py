"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("happiness_by_age.csv")

    # Load data from snapshot.
    tb = snap.read()

    tb = tb[["country", "rank", "cantril_ladder_score", "age_group"]]

    tb["year"] = 2023  # data averaged over 2021-2023

    #
    # Process data.
    #
    # Improve tables format.
    tb = tb.format(["country", "age_group", "year"])

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
