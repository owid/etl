import pandas as pd
from owid.catalog import Dataset, Table, TableMeta
from owid.catalog.utils import underscore_table
from owid.walden import Catalog as WaldenCatalog
from structlog import get_logger

from etl.helpers import Names
from etl.steps.data.converters import convert_walden_metadata

log = get_logger()

# naming conventions
N = Names(__file__)


def run(dest_dir: str) -> None:
    log.info("dummy.start")

    # retrieve raw data from walden
    walden_ds = WaldenCatalog().find_one(namespace="dummy", short_name="dummy", version="2020-01-01")
    local_file = walden_ds.ensure_downloaded()

    df = pd.read_excel(local_file, sheet_name="Full data")

    # clean and transform data
    df = clean_data(df)

    # create new dataset and reuse walden metadata
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_walden_metadata(walden_ds)
    ds.metadata.version = "2020-01-01"

    # create table with metadata from dataframe
    table_metadata = TableMeta(
        short_name=walden_ds.short_name,
        title=walden_ds.name,
        description=walden_ds.description,
    )
    tb = Table(df, metadata=table_metadata)

    # underscore all table columns
    tb = underscore_table(tb)

    ds.metadata.update_from_yaml(N.metadata_path, if_source_exists="replace")
    tb.update_metadata_from_yaml(N.metadata_path, "dummy")

    # add table to a dataset
    ds.add(tb)

    # finally save the dataset
    ds.save()

    log.info("dummy.end")


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(
        columns={
            "country": "country",
            "year": "year",
            "pop": "population",
            "gdppc": "gdp",
        }
    ).drop(columns=["countrycode"])
