import pandas as pd
from owid.catalog import Dataset, Table, TableMeta
from owid.catalog.utils import underscore_table
from owid.walden import Catalog as WaldenCatalog
from structlog import get_logger

from etl.helpers import Names
from etl.paths import DATA_DIR, REFERENCE_DATASET
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
    # Get data (statistics by period and by nature) from Walden.
    walden_ds_by_period = WaldenCatalog().find_one(namespace=NAMESPACE, short_name=WALDEN_DATASET_NAME_BY_PERIOD,
                                         version=WALDEN_VERSION)
    walden_ds_by_nature = WaldenCatalog().find_one(namespace=NAMESPACE, short_name=WALDEN_DATASET_NAME_BY_NATURE,
                                     version=WALDEN_VERSION)
    local_file_by_period = walden_ds_by_period.ensure_downloaded()
    local_file_by_nature = walden_ds_by_nature.ensure_downloaded()

    # Create dataframes from the data.
    df_by_period = pd.read_csv(local_file_by_period)
    df_by_nature = pd.read_csv(local_file_by_nature)

    # Combine both dataframes.
    df_combined = pd.merge(df_by_period, df_by_nature, how="outer", on="Year")

    # Create a new dataset and reuse Walden metadata (from one of the two datasets).
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_walden_metadata(walden_ds_by_period)
    ds.metadata.version = MEADOW_VERSION

    # Create a table with metadata with the combined dataframe.
    table_metadata = TableMeta(
        short_name=MEADOW_DATASET_NAME,
        title=MEADOW_DATASET_TITLE,
        description=walden_ds_by_period.description,
    )
    tb = Table(df_combined, metadata=table_metadata)

    # Ensure all columns are snake-case and underscore.
    tb = underscore_table(tb)    

    # Add table to new Meadow dataset.
    ds.add(tb)

    # Save the new Meadow dataset.
    ds.save()
