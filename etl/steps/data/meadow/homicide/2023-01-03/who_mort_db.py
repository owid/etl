import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import PathFinder
from etl.snapshot import Snapshot
from etl.steps.data.converters import convert_snapshot_metadata

log = get_logger()

# naming conventions
N = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("who_mort_db.start")

    # retrieve snapshot
    snap = Snapshot("homicide/2023-01-03/who_mort_db.csv")
    df = pd.read_csv(snap.path)

    # clean and transform data
    df = clean_data(df)

    # create new dataset and reuse walden metadata
    ds = Dataset.create_empty(dest_dir, metadata=convert_snapshot_metadata(snap.metadata))
    ds.metadata.version = "2023-01-03"
    df = df.reset_index().drop(columns="index")
    # # create table with metadata from dataframe and underscore all columns
    tb = Table(df, short_name=snap.metadata.short_name, underscore=True)

    # add table to a dataset
    ds.add(tb)

    # update metadata
    # ds.update_metadata(N.metadata_path)

    # finally save the dataset
    ds.save()

    log.info("who_mort_db.end")


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df[(df["Sex"] == "All") & (df["Age group code"] == "Age_all")]
    df = df.rename(columns={"Country Name": "country", "Year": "year", "Number": "number_of_deaths"}).drop(
        columns=["Region Code", "Region Name", "Country Code", "Sex", "Age group code", "Age Group"]
    )
    df["number_of_deaths"] = df["number_of_deaths"].astype(int)
    return df
