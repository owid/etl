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
    snap = paths.load_snapshot("us_state_population.csv")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    #
    tb["year"] = pd.to_datetime(tb["date"]).dt.year
    tb = tb.drop(columns=["realtime_start", "realtime_end", "date"])
    tb["value"] += tb["value"] * 1000
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tables = [tb.format(["state", "year"])]

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir,
        tables=tables,
        check_variables_metadata=True,
        default_metadata=snap.metadata,
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()
