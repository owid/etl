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
    snap = paths.load_snapshot("john_hopkins_university.csv")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    #
    ## Convert number of days since 2020-01-21 to date
    tb["date"] = pd.Timestamp("2020-01-21") + pd.to_timedelta(tb["Year"], unit="days")  # type: ignore
    tb = tb.drop(columns=["Year"])

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "date"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
