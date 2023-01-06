import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import Names
from etl.snapshot import Snapshot
from etl.steps.data.converters import convert_snapshot_metadata

log = get_logger()

# naming conventions
N = Names(__file__)


def run(dest_dir: str) -> None:
    log.info("unodc.start")

    # retrieve snapshot
    snap = Snapshot("homicide/2023-01-04/unodc.xlsx")
    df = pd.read_excel(snap.path, skiprows=2)

    # clean and transform data
    df = clean_data(df)

    # reset index so the data can be saved in feather format
    df = df.reset_index().drop(columns="index")

    # create new dataset and reuse walden metadata
    ds = Dataset.create_empty(dest_dir, metadata=convert_snapshot_metadata(snap.metadata))
    ds.metadata.version = "2023-01-04"

    # # create table with metadata from dataframe and underscore all columns
    tb = Table(df, short_name=snap.metadata.short_name, underscore=True)

    # add table to a dataset
    ds.add(tb)

    # update metadata
    ds.update_metadata(N.metadata_path)

    # finally save the dataset
    ds.save()

    log.info("unodc.end")


def clean_data(df: pd.DataFrame) -> pd.DataFrame:

    df = df[
        (df["Dimension"] == "Total")
        & (df["Category"] == "Total")
        & (df["Sex"] == "Total")
        & (df["Age"] == "Total")
        & (
            df["Indicator"].isin(
                ["Victims of intentional homicide", "Victims of Intentional Homicide - Regional Estimate"]
            )
        )
    ]
    df = df.rename(
        columns={
            "Country": "country",
            "Year": "year",
        }
    ).drop(columns=["Iso3_code"])
    return df
