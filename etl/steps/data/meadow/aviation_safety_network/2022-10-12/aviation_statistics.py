"""Generate a dataset of aviation statistics by combining the statistics "by period" and "by nature" of the Aviation
Safety Network.

"""
import pandas as pd
from owid.catalog import Dataset, Table, TableMeta
from owid.walden import Catalog as WaldenCatalog

from etl.steps.data.converters import convert_walden_metadata

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
    walden_ds_by_period = WaldenCatalog().find_one(
        namespace=NAMESPACE, short_name=WALDEN_DATASET_NAME_BY_PERIOD, version=WALDEN_VERSION
    )
    walden_ds_by_nature = WaldenCatalog().find_one(
        namespace=NAMESPACE, short_name=WALDEN_DATASET_NAME_BY_NATURE, version=WALDEN_VERSION
    )
    local_file_by_period = walden_ds_by_period.ensure_downloaded()
    local_file_by_nature = walden_ds_by_nature.ensure_downloaded()
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
    ds = Dataset.create_empty(dest_dir, metadata=convert_walden_metadata(walden_ds_by_period))
    ds.metadata.version = MEADOW_VERSION
    ds.metadata.short_name = MEADOW_DATASET_NAME

    table_metadata = TableMeta(
        short_name=MEADOW_DATASET_NAME,
        title=MEADOW_DATASET_TITLE,
        description=walden_ds_by_period.description,
    )
    tab = Table(
        df_combined,
        metadata=table_metadata,
        underscore=True,
    )
    ds.add(tab)

    # Save the new Meadow dataset.
    ds.save()
