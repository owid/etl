import re
import pandas as pd

from structlog import get_logger
from pathlib import Path
from etl.steps.data.converters import convert_walden_metadata
from owid.walden import Catalog
from owid.catalog import Dataset, Table, TableMeta
from owid.catalog.utils import underscore

BASE_URL = "https://unstats.un.org/sdgapi"
log = get_logger()


def run(dest_dir: str, query: str = "") -> None:
    # retrieves raw data from walden
    version = Path(__file__).parent.stem
    fname = Path(__file__).stem
    namespace = Path(__file__).parent.parent.stem

    version = "2022-05-26"
    fname = "un_sdg"
    namespace = "un_sdg"
    walden_ds = Catalog().find_one(
        namespace=namespace, short_name=fname, version=version
    )

    log.info("un_sdg.start")
    local_file = walden_ds.ensure_downloaded()
    # NOTE: using feather format instead of csv would make it 5x smaller and
    # load significantly faster
    df = pd.read_csv(local_file, low_memory=False)

    if query:
        df = df.query(query)

    log.info("un_sdg.load_and_clean")
    df = load_and_clean(df)
    log.info("Size of dataframe", rows=df.shape[0], colums=df.shape[1])
    df.columns = [underscore(c) for c in df.columns]
    df = df.reset_index()
    ds = Dataset.create_empty(dest_dir)

    ds.metadata = convert_walden_metadata(walden_ds)
    tb = Table(df)
    tb.metadata = TableMeta(
        short_name=Path(__file__).stem,
        title=walden_ds.name,
        description=walden_ds.description,
    )
    ds.add(tb)
    ds.save()
    log.info("un_sdg.end")


def load_and_clean(original_df: pd.DataFrame) -> pd.DataFrame:
    # Load and clean the data
    log.info("Reading in original data...")
    original_df = original_df.copy(deep=False)

    # removing values that aren't numeric e.g. Null and N values
    original_df.dropna(subset=["Value"], inplace=True)
    original_df.dropna(subset=["TimePeriod"], how="all", inplace=True)
    original_df = original_df[
        pd.to_numeric(original_df["Value"], errors="coerce").notnull()
    ]
    original_df.rename(
        columns={"GeoAreaName": "Country", "TimePeriod": "Year"}, inplace=True
    )
    original_df = original_df.rename(columns=lambda k: re.sub(r"[\[\]]", "", k))  # type: ignore
    return original_df


if __name__ == "__main__":
    # test script for a single indicator with `python etl/steps/data/meadow/un_sdg/2022-05-26/un_sdg.py`
    run("/tmp/un_sdg", query="Indicator == '1.1.1'")
