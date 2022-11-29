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
N = Names("etl/steps/data/meadow/un/2021-12-20/un_igme.py")


def run(dest_dir: str) -> None:
    log.info("un_igme.start")

    # retrieve raw data from walden
    walden_ds = WaldenCatalog().find_one(namespace="un", short_name="un_igme", version="2021")
    local_file = walden_ds.ensure_downloaded()

    df = pd.read_csv(local_file, low_memory=False)

    # clean and transform data
    df = clean_data(df)

    # create new dataset and reuse walden metadata
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_walden_metadata(walden_ds)
    ds.metadata.version = "2021-12-20"

    # create table with metadata from dataframe
    table_metadata = TableMeta(
        short_name=ds.metadata.short_name,
        title=ds.metadata.title,
        description=ds.metadata.description,
    )
    tb = Table(df, metadata=table_metadata)

    # underscore all table columns
    tb = underscore_table(tb)
    tb = tb.reset_index()

    # add table to a dataset
    ds.add(tb)

    # finally save the dataset
    ds.save()

    log.info("un_igme.end")


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy(deep=True)
    df = df[
        (df["Series Name"] == "UN IGME estimate")
        & (df["Wealth Quintile"] == "Total")
        & (df["Age Group of Women"].isna())
        & (df["Time Since First Birth"].isna())
        & (df["DEFINITION"].isna())
    ]
    df["year"] = df["TIME_PERIOD"].str[:4].astype(int)
    return df.rename(
        columns={
            "Geographic area": "country",
            "Indicator": "indicator",
            "Sex": "sex",
            "Regional group": "regional_group",
            "OBS_VALUE": "value",
            "Unit of measure": "unit",
            "LOWER_BOUND": "lower_bound",
            "UPPER_BOUND": "upper_bound",
        }
    ).drop(
        columns=[
            "Wealth Quintile",
            "Series Name",
            "Series Year",
            "TIME_PERIOD",
            "COUNTRY_NOTES",
            "CONNECTION",
            "DEATH_CATEGORY",
            "CATEGORY",
            "Observation Status",
            "Series Category",
            "Series Type",
            "STD_ERR",
            "REF_DATE",
            "Age Group of Women",
            "Time Since First Birth",
            "DEFINITION",
            "INTERVAL",
            "Series Method",
            "STATUS",
            "YEAR_TO_ACHIEVE",
            "Model Used",
        ]
    )
