"""Generate a dataset of aviation statistics using data from the public spreadsheet generated by the Aviation Safety
Network.

"""

import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

MEADOW_DATASET_TITLE = "Aviation statistics"


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Get data (statistics by period and by nature) from Snapshot.
    snap: Snapshot = paths.load_dependency("aviation_statistics.csv")  # type: ignore
    df = pd.read_csv(snap.path).rename(columns={"year": "year"})

    #
    # Process data.
    #
    # Add a country column (that only contains "World").
    df["country"] = "World"

    # Set an appropriate index and sort conveniently.
    df = df.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    # Create a table with metadata.
    tb = Table(df, short_name=paths.short_name, underscore=True)
    tb.metadata.title = MEADOW_DATASET_TITLE
    tb.metadata.description = snap.metadata.description

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()