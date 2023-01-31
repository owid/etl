import pandas as pd
from owid.catalog import Dataset, Table, TableMeta
from owid.catalog.utils import underscore_table
from owid.walden import Catalog as WaldenCatalog
from structlog import get_logger

from etl.helpers import PathFinder
from etl.steps.data.converters import convert_walden_metadata

log = get_logger()

# Details of dataset to be exported.
VERSION = "2019-12-03"
DATASET_NAME = "co2_mitigation_curves"
# Details of dataset(s) to be imported.
WALDEN_VERSION = "2019-12-03"
# Load naming conventions.
N = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("co2_mitigation_curves.start")

    # Load raw dataset on mitigation curves for 2 Celsius.
    walden_ds_2celsius = WaldenCatalog().find_one(
        namespace="andrew", short_name="co2_mitigation_curves_2celsius", version=WALDEN_VERSION
    )
    local_file_2celsius = walden_ds_2celsius.ensure_downloaded()
    df_2celsius = pd.read_csv(local_file_2celsius)

    # Load raw dataset on mitigation curves for 1.5 Celsius.
    walden_ds_1p5celsius = WaldenCatalog().find_one(
        namespace="andrew", short_name="co2_mitigation_curves_1p5celsius", version=WALDEN_VERSION
    )
    local_file_1p5celsius = walden_ds_1p5celsius.ensure_downloaded()
    df_1p5celsius = pd.read_csv(local_file_1p5celsius)

    # create new dataset and reuse walden metadata
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_walden_metadata(walden_ds_2celsius)
    ds.metadata.version = VERSION
    ds.metadata.short_name = DATASET_NAME

    # Create tables with metadata from dataframe for 2 Celsius.
    table_metadata_2celsius = TableMeta(
        short_name=walden_ds_2celsius.short_name,
        title=walden_ds_2celsius.name,
        description=walden_ds_2celsius.description,
    )
    tb_2celsius = Table(df_2celsius, metadata=table_metadata_2celsius)
    # Underscore all table columns.
    tb_2celsius = underscore_table(tb_2celsius)

    # Create tables with metadata from dataframe for 1.5 Celsius.
    table_metadata_1p5celsius = TableMeta(
        short_name=walden_ds_1p5celsius.short_name,
        title=walden_ds_1p5celsius.name,
        description=walden_ds_1p5celsius.description,
    )
    tb_1p5celsius = Table(df_1p5celsius, metadata=table_metadata_1p5celsius)
    # Underscore all table columns.
    tb_1p5celsius = underscore_table(tb_1p5celsius)

    # Add tables to the new dataset.
    ds.add(tb_2celsius)
    ds.add(tb_1p5celsius)

    # finally save the dataset
    ds.save()

    log.info("co2_mitigation_curves.end")
