import pandas as pd
from owid.catalog import Dataset, Table, TableMeta
from owid.catalog.utils import underscore_table
from owid.walden import Catalog as WaldenCatalog
from structlog import get_logger

from etl.helpers import PathFinder
from etl.steps.data.converters import convert_walden_metadata

log = get_logger()

# naming conventions
N = PathFinder(__file__)

# Version of meadow dataset to be created.
VERSION = "2017-01-01"
# Walden version of the dataset.
WALDEN_VERSION = "2017-01-01"


def run(dest_dir: str) -> None:
    log.info("global_primary_energy.start")

    # Retrieve raw data from walden.
    walden_ds = WaldenCatalog().find_one(namespace="smil", short_name="global_primary_energy", version=WALDEN_VERSION)
    local_file = walden_ds.ensure_downloaded()
    df = pd.read_csv(local_file)

    # Create a new meadow dataset and reuse walden metadata.
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_walden_metadata(walden_ds)
    ds.metadata.version = VERSION

    # Create a new table with metadata from the dataset.
    table_metadata = TableMeta(short_name=walden_ds.short_name, title=walden_ds.name, description=walden_ds.description)
    tb = Table(df, metadata=table_metadata)
    # Use the current names of the columns as the variable titles in the metadata.
    for column in tb.columns:
        tb[column].metadata.title = column
    # Change all columns to be lower snake case.
    tb = underscore_table(tb)
    # Set table index.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index()

    # Add table to dataset.
    ds.add(tb)

    # Save dataset.
    ds.save()

    log.info("global_primary_energy.end")
