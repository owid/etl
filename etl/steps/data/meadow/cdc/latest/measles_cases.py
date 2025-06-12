"""Load a snapshot and create a meadow dataset."""

import json

import pandas as pd
from owid.catalog.tables import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("measles_cases.json")
    origins = [snap.metadata.origin]
    # Load JSON data from snapshot.
    with open(snap.path, "r", encoding="utf-8-sig") as f:
        data = json.load(f)
        tb = pd.DataFrame(data)
    #
    # Add country
    tb["country"] = "United States"
    tb = Table(tb, underscore=False)
    for col in tb.columns:
        tb[col].metadata.origins = origins
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tables = [tb.format(["country", "year", "filter"], short_name=paths.short_name)]

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
