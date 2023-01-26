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
    log.info("child_mortality.start")

    # retrieve raw data from walden
    walden_ds = WaldenCatalog().find_one(namespace="ihme_gbd", short_name="child_mortality", version="2020-12-19")
    local_file = walden_ds.ensure_downloaded()

    df = pd.read_feather(local_file)
    df = df.drop(columns="index")

    # clean and transform data
    df = clean_data(df)

    # create new dataset and reuse walden metadata
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_walden_metadata(walden_ds)

    # create table with metadata from dataframe
    table_metadata = TableMeta(
        short_name=walden_ds.short_name,
        title=walden_ds.name,
        description=walden_ds.description,
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
