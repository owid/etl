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
    log.info("child_mortality.start")

    # retrieve raw data from walden
    snap = Snapshot(f"ihme_gbd/2020-12-19/child_mortality.csv")
    local_file = str(snap.path)

    df = pd.read_feather(local_file)
    df = df.drop(columns="index")

    # clean and transform data
    df = clean_data(df)

    # create new dataset and reuse walden metadata
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_snapshot_metadata(snap.metadata)

    # create table with metadata from dataframe
    table_metadata = TableMeta(
        short_name=snap.metadata.short_name,
        title=snap.metadata.name,
        description=snap.metadata.description,
    )
    tb = Table(df, metadata=table_metadata)

    # underscore all table columns
    tb = underscore_table(tb)

    tb.update_metadata_from_yaml(N.metadata_path, "child_mortality")
    tb = tb.reset_index()
    # add table to a dataset
    ds.add(tb)

    # finally save the dataset
    ds.save()

    log.info("child_mortality.end")


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.rename(
            columns={
                "location_name": "country",
                "year_id": "year",
                "val": "value",
            }
        )
        .drop(columns=["location_id", "sex_id", "age_group_id", "measure_id", "metric_id", "upper", "lower"])
        .drop_duplicates()
    )
