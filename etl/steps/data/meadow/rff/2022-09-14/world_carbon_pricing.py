import pandas as pd
from owid.catalog import Dataset, Table, TableMeta
from owid.catalog.utils import underscore_table
from owid.walden import Catalog as WaldenCatalog

from etl.steps.data.converters import convert_walden_metadata

# Version for dataset to be created.
VERSION = "2022-09-14"
# Version of dataset in walden.
WALDEN_VERSION = "2022-09-14"


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load data from walden.
    walden_ds = WaldenCatalog().find_one(namespace="rff", short_name="world_carbon_pricing", version=WALDEN_VERSION)
    local_file = walden_ds.ensure_downloaded()
    df = pd.read_csv(local_file, dtype=object)

    #
    # Save outputs.
    #
    # Create new dataset with metadata from walden.
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_walden_metadata(walden_ds)
    ds.metadata.version = VERSION

    # Create new table with metadata.
    table_metadata = TableMeta(short_name=walden_ds.short_name, title=walden_ds.name, description=walden_ds.description)
    tb = Table(df, metadata=table_metadata)

    # Prepare table and add it to the dataset.
    tb = underscore_table(tb)
    ds.add(tb)

    # Save the dataset.
    ds.save()
