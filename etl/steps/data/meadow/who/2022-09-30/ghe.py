import pandas as pd
from owid.catalog import Dataset, Table, TableMeta
from owid.catalog.utils import underscore_table
from structlog import get_logger

from etl.helpers import Names
from etl.snapshot import Snapshot
from etl.steps.data.converters import convert_snapshot_metadata

log = get_logger()

# naming conventions
N = Names(__file__)


def run(dest_dir: str) -> None:
    log.info("ghe.start")

    # retrieve raw data from walden
    snap = Snapshot("who/2022-09-30/ghe.csv")
    local_file = str(snap.path)

    df = pd.read_feather(local_file)

    # clean and transform data
    df = clean_data(df)

    # create new dataset and reuse walden metadata
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_snapshot_metadata(snap.metadata)
    ds.metadata.version = "2022-09-30"

    # create table with metadata from dataframe
    table_metadata = TableMeta(
        short_name=snap.metadata.short_name,
        title=snap.metadata.name,
        description=snap.metadata.description,
    )
    tb = Table(df, metadata=table_metadata)

    # underscore all table columns
    tb = underscore_table(tb)

    # ds.metadata.update_from_yaml(N.metadata_path)
    tb.update_metadata_from_yaml(N.metadata_path, "ghe")

    # add table to a dataset
    ds.add(tb)

    # finally save the dataset
    ds.save()

    log.info("ghe.end")


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(
        columns={
            "DIM_COUNTRY_CODE": "country",
            "DIM_YEAR_CODE": "year",
            "DIM_AGEGROUP_CODE": "age_group",
            "DIM_SEX_CODE": "sex",
            "DIM_GHECAUSE_TITLE": "cause",
            "VAL_DALY_RATE100K_NUMERIC": "daly_rate100k",
            "VAL_DALY_COUNT_NUMERIC": "daly_count",
            "VAL_DEATHS_RATE100K_NUMERIC": "death_rate100k",
            "VAL_DEATHS_COUNT_NUMERIC": "death_count",
        }
    ).drop(columns=["index"])
