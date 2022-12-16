import pandas as pd
from owid.catalog import Dataset, Table, TableMeta
from owid.catalog.utils import underscore_table
from structlog import get_logger

from etl.helpers import Names
from etl.snapshot import Snapshot
from etl.steps.data.converters import convert_snapshot_metadata

log = get_logger()

# Details of dataset to be exported.
VERSION = "2019-12-03"
DATASET_NAME = "co2_mitigation_curves"
# Details of dataset(s) to be imported.
WALDEN_VERSION = "2019-12-03"
# Load naming conventions.
N = Names(__file__)


def run(dest_dir: str) -> None:
    log.info("co2_mitigation_curves.start")

    # Load raw dataset on mitigation curves for 2 Celsius.
    snap_2celsius = Snapshot(f"andrew/{WALDEN_VERSION}/co2_mitigation_curves_2celsius.csv")
    df_2celsius = pd.read_csv(snap_2celsius.path)

    # Load raw dataset on mitigation curves for 1.5 Celsius.
    snap_1p5celsius = Snapshot(f"andrew/{WALDEN_VERSION}/co2_mitigation_curves_1p5celsius.csv")
    df_1p5celsius = pd.read_csv(snap_1p5celsius.path)

    # create new dataset and reuse walden metadata
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_snapshot_metadata(snap_2celsius.metadata)
    ds.metadata.version = VERSION

    # Create tables with metadata from dataframe for 2 Celsius.
    table_metadata_2celsius = TableMeta(
        short_name=snap_2celsius.metadata.short_name,
        title=snap_2celsius.metadata.name,
        description=snap_2celsius.metadata.description,
    )
    tb_2celsius = Table(df_2celsius, metadata=table_metadata_2celsius)
    # Underscore all table columns.
    tb_2celsius = underscore_table(tb_2celsius)

    # Create tables with metadata from dataframe for 1.5 Celsius.
    table_metadata_1p5celsius = TableMeta(
        short_name=snap_1p5celsius.metadata.short_name,
        title=snap_1p5celsius.metadata.name,
        description=snap_1p5celsius.metadata.description,
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
