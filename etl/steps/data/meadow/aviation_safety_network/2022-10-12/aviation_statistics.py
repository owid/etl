"""Generate a dataset of aviation statistics by combining the statistics "by period" and "by nature" of the Aviation
Safety Network.

"""
import pandas as pd
from owid.catalog import Dataset, Table, TableMeta

from etl.snapshot import Snapshot
from etl.steps.data.converters import convert_snapshot_metadata

NAMESPACE = "aviation_safety_network"
# Details for input datasets.
WALDEN_DATASET_NAME_BY_PERIOD = "aviation_statistics_by_period"
WALDEN_DATASET_NAME_BY_NATURE = "aviation_statistics_by_nature"
WALDEN_VERSION = "2022-10-12"
# Details for output dataset.
MEADOW_VERSION = WALDEN_VERSION
MEADOW_DATASET_NAME = "aviation_statistics"
MEADOW_DATASET_TITLE = "Aviation statistics"


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Get data (statistics by period and by nature) from Walden.
    snap_by_period = Snapshot(f"{NAMESPACE}/{WALDEN_VERSION}/{WALDEN_DATASET_NAME_BY_PERIOD}.csv")
    snap_by_nature = Snapshot(f"{NAMESPACE}/{WALDEN_VERSION}/{WALDEN_DATASET_NAME_BY_PERIOD}.csv")
    local_file_by_period = snap_by_period.path
    local_file_by_nature = snap_by_nature.path
    # Create dataframes from the data.
    df_by_period = pd.read_csv(local_file_by_period).rename(columns={"Year": "year"})
    df_by_nature = pd.read_csv(local_file_by_nature).rename(columns={"Year": "year"})

    #
    # Process data.
    #
    # Combine both dataframes.
    df_combined = pd.merge(df_by_period, df_by_nature, how="outer", on="year")

    # Add a country column (that only contains "World").
    df_combined["country"] = "World"

    # Set an appropriate index and sort conveniently.
    df_combined = df_combined.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    #
    # Save outputs.
    #
    ds = Dataset.create_empty(dest_dir, metadata=convert_snapshot_metadata(snap_by_period.metadata))
    ds.metadata.version = MEADOW_VERSION

    table_metadata = TableMeta(
        short_name=MEADOW_DATASET_NAME,
        title=MEADOW_DATASET_TITLE,
        description=snap_by_period.metadata.description,
    )
    tab = Table(
        df_combined,
        metadata=table_metadata,
        underscore=True,
    )
    ds.add(tab)

    # Save the new Meadow dataset.
    ds.save()
