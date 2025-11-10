"""Load a snapshot and create a meadow dataset."""

import pandas as pd

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    """Create meadow dataset from snapshot."""
    # Load inputs
    snap = paths.load_snapshot("ireland_metered_consumption.csv")

    # Read data from snapshot
    tb = snap.read()

    # Convert quarter string (e.g., "2015Q1") to date
    # Use period_to_timestamp to convert to start of quarter
    tb["date"] = pd.PeriodIndex(tb["Quarter"], freq="Q").to_timestamp()

    tb = tb.drop(columns=["Quarter", "Statistic Label", "UNIT"])

    tb["country"] = "Ireland"
    tb = tb.format(["date", "electricity_consumption"])

    # Save outputs
    ds_meadow = create_dataset(
        dest_dir,
        tables=[tb],
        check_variables_metadata=True,
        default_metadata=snap.metadata,
    )
    ds_meadow.save()
