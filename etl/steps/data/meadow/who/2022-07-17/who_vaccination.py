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


def run(dest_dir: str) -> None:
    log.info("who_vaccination.start")

    # retrieve raw data from walden
    walden_ds = WaldenCatalog().find_one(namespace="who", short_name="who_vaccination", version="2022-07-17")
    local_file = walden_ds.ensure_downloaded()

    df = pd.read_excel(local_file, sheet_name="Data")

    # clean and transform data
    df = clean_data(df)

    # create new dataset and reuse walden metadata
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_walden_metadata(walden_ds)
    ds.metadata.version = "2022-07-17"

    # create table with metadata from dataframe
    table_metadata = TableMeta(
        short_name=walden_ds.short_name,
        title=walden_ds.name,
        description=walden_ds.description,
    )
    tb = Table(df, metadata=table_metadata)

    # underscore all table columns
    tb = underscore_table(tb)

    ds.metadata.update_from_yaml(N.metadata_path)
    tb.update_metadata_from_yaml(N.metadata_path, "who_vaccination")
    tb = tb.reset_index()
    # add table to a dataset
    ds.add(tb)

    # finally save the dataset
    ds.save()

    log.info("who_vaccination.end")


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    df = underscore_table(Table(df))
    df = df.dropna(subset="name")
    return df.rename(columns={"name": "country"}).drop(columns=["code"])
