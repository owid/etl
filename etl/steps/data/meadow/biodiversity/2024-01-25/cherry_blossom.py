from datetime import datetime
from typing import cast

from owid.catalog import Table
from owid.catalog import processing as pr
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

log = get_logger()

# naming conventions
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("cherry_blossom.start")

    # retrieve snapshot
    snap = cast(Snapshot, paths.load_dependency("cherry_blossom.xls"))
    tb = snap.read(skiprows=25)

    # clean and transform data
    tb = clean_data(tb)
    tb = add_data_for_recent_years(tb)
    tb = convert_date(tb)

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds = create_dataset(dest_dir, tables=[tb.set_index(["country", "year"])], default_metadata=snap.metadata)

    # finally save the dataset
    ds.save()

    log.info("cherry_blossom.end")


def clean_data(tb: Table) -> Table:
    tb = tb.dropna(subset=["Full-flowering date (DOY)"])
    tb["country"] = "Japan"
    tb = tb.rename(columns={"AD": "year"}).drop(columns=["Source code", "Data type code", "Reference Name"])
    return tb


def convert_date(tb: Table) -> Table:
    """
    The full flowering date is formated like MDD, we should change this to day of the year for better biological meaning. For example the 4th April is shown as 404.
    In this function we:
     - Zero pad the year so it is 4 digits long - the data starts in 812
     - Zero pad the full-flowering date so it is 4 digits long
     - Combine these year and the full-flowering date so 1st April 812 would look like 08120401
     - Convert this into a date format and extract the Julian day (day of the year)
    """
    tb["year"] = tb["year"].astype("str")
    tb["year"] = tb["year"].str.zfill(4)
    tb["Full-flowering date"] = tb["Full-flowering date"].astype(float).astype("Int64").astype("str").str.zfill(4)
    tb["date_combine"] = tb["year"] + tb["Full-flowering date"]

    tb["Full-flowering date"] = tb["date_combine"].apply(lambda x: datetime.strptime(x, "%Y%m%d").strftime("%j"))

    tb = tb.drop(columns=["Full-flowering date (DOY)", "date_combine"])

    return tb


def add_data_for_recent_years(tb: Table) -> Table:
    """
    Not sure if this is the best way to do this...
    Adding data for the years 2016-21, from here:

    http://atmenv.envi.osakafu-u.ac.jp/aono/kyophenotemp4/
    """

    tb_new = Table(
        {
            "country": ["Japan", "Japan", "Japan", "Japan", "Japan", "Japan", "Japan", "Japan"],
            "year": ["2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023"],
            "Full-flowering date": ["404", "409", "330", "405", "401", "326", "330", "324"],
        }
    )

    tb = pr.concat([tb, tb_new])

    return tb
