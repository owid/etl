"""Load a snapshot and create a meadow dataset."""

import pandas as pd

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    """Create meadow dataset."""
    # Load snapshot.
    snap = paths.load_snapshot("ai_indicator.csv")
    tb = snap.read()

    # Rename columns to snake_case.
    tb = tb.rename(
        columns={
            "Use of AI": "date",
            "Daily AI users": "daily_ai_users",
            "Frequent AI users": "frequent_ai_users",
            "Total AI users": "total_ai_users",
        }
    )

    # Parse date column (format: M/D/YY).
    tb["date"] = pd.to_datetime(tb["date"], format="%m/%d/%y").dt.strftime("%Y-%m-%d")

    tb = tb.format(["date"])

    # Save meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)
    ds_meadow.save()
