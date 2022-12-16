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
    log.info("penn_world_table.start")

    # retrieve raw data from walden
    snap = Snapshot(f"ggdc/2021-06-18/penn_world_table.csv")
    local_file = str(snap.path)

    df = pd.read_excel(local_file, sheet_name="Data")

    # # clean and transform data
    # df = clean_data(df)

    # create new dataset and reuse walden metadata
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_snapshot_metadata(snap.metadata)
    ds.metadata.version = "2022-11-28"

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
    tb.update_metadata_from_yaml(N.metadata_path, "penn_world_table")

    # add table to a dataset
    ds.add(tb)

    # finally save the dataset
    ds.save()

    log.info("penn_world_table.end")


# def clean_data(df: pd.DataFrame) -> pd.DataFrame:
#     return df.rename(
#         columns={
#             "country": "country",
#             "year": "year",
#             "pop": "population",
#             "gdppc": "gdp",
#         }
#     ).drop(columns=["countrycode"])
