import numpy as np
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
    log.info("postnatal_care.start")

    # retrieve raw data from walden
    snap = Snapshot(f"postnatal_care/2022-09-19/postnatal_care.csv")
    local_file = str(snap.path)

    df = pd.read_csv(local_file)
    df = df[df["Series Code"].notna()]

    # clean and transform data
    df = clean_data(df)

    # create new dataset and reuse walden metadata
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_snapshot_metadata(snap.metadata)
    ds.metadata.version = "2022-09-19"

    # create table with metadata from dataframe
    table_metadata = TableMeta(
        short_name=snap.metadata.short_name,
        title=snap.metadata.name,
        description=snap.metadata.description,
    )
    tb = Table(df, metadata=table_metadata)

    # underscore all table columns
    tb = underscore_table(tb)

    ds.metadata.update_from_yaml(N.metadata_path, if_source_exists="replace")
    tb.update_metadata_from_yaml(N.metadata_path, "postnatal_care")

    # add table to a dataset
    ds.add(tb)

    # finally save the dataset
    ds.save()

    log.info("postnatal_care.end")


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop(columns=["Country Code", "Series Name", "Series Code"])

    cols = df.columns[1:].str[:4].tolist()
    df.columns = ["country"] + cols
    df = df.replace("..", np.nan)
    df = pd.melt(df, id_vars="country", value_vars=cols)
    df = df.rename(columns={"variable": "year", "value": "postnatal_care_coverage"})
    return df
