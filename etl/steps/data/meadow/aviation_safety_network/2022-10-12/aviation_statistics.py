"""Generate a dataset of aviation statistics by combining the statistics "by period" and "by nature" of the Aviation
Safety Network.

"""
from typing import cast

import pandas as pd
from owid.catalog import Table, TableMeta

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Details for output dataset.
MEADOW_DATASET_NAME = "aviation_statistics"
MEADOW_DATASET_TITLE = "Aviation statistics"

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Get data (statistics by period and by nature) from Snapshot.
    snap_by_period = cast(Snapshot, paths.load_dependency("aviation_statistics_by_period.csv"))
    snap_by_nature = cast(Snapshot, paths.load_dependency("aviation_statistics_by_nature.csv"))

    # Create dataframes from the data.
    df_by_period = pd.read_csv(snap_by_period.path).rename(columns={"Year": "year"})
    df_by_nature = pd.read_csv(snap_by_nature.path).rename(columns={"Year": "year"})

    #
    # Process data.
    #
    # Combine both dataframes.
    df_combined = pd.merge(df_by_period, df_by_nature, how="outer", on="year")

    # Add a country column (that only contains "World").
    df_combined["country"] = "World"

    # Set an appropriate index and sort conveniently.
    df_combined = df_combined.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    # Process data
    tb = Table(
        df_combined,
        metadata=TableMeta(
            short_name=MEADOW_DATASET_NAME,
            title=MEADOW_DATASET_TITLE,
            description=snap_by_period.metadata.description,
        ),
        underscore=True,
    )

    #
    # Save outputs.
    #
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap_by_period.metadata)

    # Save the new Meadow dataset.
    ds_meadow.save()
