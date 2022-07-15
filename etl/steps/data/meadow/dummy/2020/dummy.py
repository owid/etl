import pandas as pd
from owid.catalog import Dataset, Table, TableMeta
from owid.catalog.utils import underscore_table
from owid.walden import Catalog as WaldenCatalog
from structlog import get_logger

from etl.paths import DATA_DIR, REFERENCE_DATASET
from etl.steps.data.converters import convert_walden_metadata

log = get_logger()def load_countries_regions() -> Table:
    # load countries regions (e.g. to map from iso codes to country names)
    reference_dataset = Dataset(REFERENCE_DATASET)
    return reference_dataset["countries_regions"]
def load_population() -> Table:
    # load countries regions (e.g. to map from iso codes to country names)
    indicators = Dataset(DATA_DIR / "garden/owid/latest/key_indicators")
    return indicators["population"]



def run(dest_dir: str) -> None:
    log.info("dummy.start")

    # retrieve raw data from walden
    walden_ds = WaldenCatalog().find_one(
        namespace="dummy", short_name="dummy", version="2020"
    )
    local_file = walden_ds.ensure_downloaded()
    df = pd.read_excel(local_file)

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

    # add table to a dataset
    ds.add(tb)

    # finally save the dataset
    ds.save()

    log.info("dummy.end")


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    return df
