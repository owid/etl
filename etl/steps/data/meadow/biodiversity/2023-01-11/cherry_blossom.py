from datetime import datetime

from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

log = get_logger()

# naming conventions
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("cherry_blossom.start")

    # retrieve snapshot
    snap = Snapshot("biodiversity/2023-01-11/cherry_blossom.csv")
    tb = snap.read()

    # clean and transform data
    tb = clean_data(tb)
    tb = convert_date(tb)

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds = create_dataset(dest_dir, tables=[tb.set_index(["country", "year"])], default_metadata=snap.metadata)

    # finally save the dataset
    ds.save()

    log.info("cherry_blossom.end")


def clean_data(df: Table) -> Table:
    df = df.dropna(subset=["Entity", "Full-flowering date"])

    return df.rename(columns={"Entity": "country", "Year": "year"}).drop(
        columns=["Source code", "Data type code", "Reference Name", "Unnamed: 7"]
    )


def convert_date(df: Table) -> Table:
    """
    The full flowering date is formated like MDD, we should change this to day of the year for better biological meaning. For example the 4th April is shown as 404.
    In this function we:
     - Zero pad the year so it is 4 digits long - the data starts in 812
     - Zero pad the full-flowering date so it is 4 digits long
     - Combine these year and the full-flowering date so 1st April 812 would look like 08120401
     - Convert this into a date format and extract the Julian day (day of the year)
    """

    df["year"] = df["year"].str.zfill(4)
    df["Full-flowering date"] = df["Full-flowering date"].astype("Int64").astype("str").str.zfill(4)
    df["date_combine"] = df["year"] + df["Full-flowering date"]

    df["Full-flowering date"] = df["date_combine"].apply(lambda x: datetime.strptime(x, "%Y%m%d").strftime("%j"))

    df = df.drop(columns=["Full-flowering date (DOY)", "date_combine"])

    return df
