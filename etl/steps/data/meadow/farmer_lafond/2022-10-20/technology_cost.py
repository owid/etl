import pandas as pd
from owid.catalog import Dataset, Table, TableMeta
from owid.catalog.utils import underscore_table
from owid.walden import Catalog as WaldenCatalog

from etl.helpers import Names
from etl.steps.data.converters import convert_walden_metadata

# Walden dataset version.
WALDEN_VERSION = "2022-10-20"
VERSION = WALDEN_VERSION
# Get naming conventions.
N = Names(__file__)


def run(dest_dir: str) -> None:
    # Load raw data from Walden.
    walden_ds = WaldenCatalog().find_one(
        namespace="farmer_lafond", short_name="technology_cost", version=WALDEN_VERSION
    )
    local_file = walden_ds.ensure_downloaded()
    df = pd.read_csv(local_file)

    # Set an appropriate index and sort conveniently.
    df = df.set_index(["item", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    # Create new dataset and reuse Walden metadata.
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_walden_metadata(walden_ds)
    ds.metadata.version = VERSION

    # Create new table with metadata from dataframe.
    table_metadata = TableMeta(
        short_name=walden_ds.short_name,
        title=walden_ds.name,
        description=walden_ds.description,
    )
    tb = Table(df, metadata=table_metadata)

    # Underscore all table columns.
    tb = underscore_table(tb)

    # Add table to new Meadow dataset and save dataset.
    ds.add(tb)
    ds.save()
