from datetime import datetime

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import PathFinder
from etl.snapshot import Snapshot
from etl.steps.data.converters import convert_snapshot_metadata

log = get_logger()

# naming conventions
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("cherry_blossom.start")

    # retrieve snapshot
    snap = Snapshot("biodiversity/2023-01-11/cherry_blossom.csv")
    df = pd.read_csv(snap.path)

    # clean and transform data
    df = clean_data(df)

    df = convert_date(df)
    # create new dataset and reuse walden metadata
    ds = Dataset.create_empty(dest_dir, metadata=convert_snapshot_metadata(snap.metadata))
    ds.metadata.version = "2023-01-11"

    df = df.reset_index(drop=True)
    # # create table with metadata from dataframe and underscore all columns
    tb = Table(df, short_name=snap.metadata.short_name, underscore=True)

    # add table to a dataset
    ds.add(tb)

    # update metadata
    ds.update_metadata(paths.metadata_path)

    # finally save the dataset
    ds.save()

    log.info("cherry_blossom.end")


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(subset=["Entity", "Full-flowering date"])

    return df.rename(columns={"Entity": "country", "Year": "year"}).drop(
        columns=["Source code", "Data type code", "Reference Name", "Unnamed: 7"]
    )


def convert_date(df: pd.DataFrame) -> pd.DataFrame:
    """
    The full flowering date is formated like MDD, we should change this to day of the year for better biological meaning
     - Zero pad the year so it is 4 digits long
     - Zero pad the full-flowering date so it is 4 digits long
    """

    df["year"] = df["year"].str.zfill(4)
    df["Full-flowering date"] = df["Full-flowering date"].astype("Int64").astype("str").str.zfill(4)
    df["date_combine"] = df["year"] + df["Full-flowering date"]

    df["Full-flowering date"] = list(map(lambda x: datetime.strptime(x, "%Y%m%d").strftime("%j"), df["date_combine"]))

    df = df.drop(columns=["Full-flowering date (DOY)", "date_combine"])

    return df
