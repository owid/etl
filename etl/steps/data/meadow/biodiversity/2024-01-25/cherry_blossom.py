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
    tb = tb.set_index(["country", "year"], verify_integrity=True)
    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # finally save the dataset
    ds.save()

    log.info("cherry_blossom.end")


def clean_data(tb: Table) -> Table:
    """Removes rows with missing flowering dates and renames relevant columns."""
    tb = tb.dropna(subset=["Full-flowering date (DOY)"])
    tb["country"] = "Japan"
    tb = tb.rename(columns={"AD": "year"}).drop(columns=["Source code", "Data type code", "Reference Name"])
    return tb


def convert_date(tb: Table) -> Table:
    """
    Convert full flowering dates from MDD format to day of the year.
    This function transforms the full flowering date format from MDD (e.g., 404 for April 4th) to the Julian day format.
    It ensures the year is four digits long and combines it with the zero-padded full-flowering date to create a date object,
    from which the Julian day is extracted.
    Args:
        tb (Table): The input table with the full-flowering date in MDD format.
    Returns:
        Table: The table with the full-flowering date converted to Julian day format.
    """
    tb["year_zpad"] = tb["year"].astype("str")
    tb["year_zpad"] = tb["year_zpad"].str.zfill(4)
    tb["Full-flowering date"] = tb["Full-flowering date"].astype(float).astype("Int64").astype("str").str.zfill(4)
    tb["date_combine"] = tb["year_zpad"] + tb["Full-flowering date"]

    tb["Full-flowering date"] = tb["date_combine"].apply(lambda x: datetime.strptime(x, "%Y%m%d").strftime("%j"))

    tb = tb.drop(columns=["Full-flowering date (DOY)", "date_combine", "year_zpad"])

    return tb


def add_data_for_recent_years(tb: Table) -> Table:
    """
    Add cherry blossom full flowering dates for recent years not covered in the original dataset.
    This function manually adds data for the years 2016 to 2023. The data for 2016 to 2021 is sourced from the official dataset website,
    while the data for 2022 and 2023 comes from personal communication with the dataset author.
    Args:
        tb (Table): The input table containing cherry blossom data.
    Returns:
        Table: The table with added data for recent years.
    """

    tb_new = Table(
        {
            "country": ["Japan", "Japan", "Japan", "Japan", "Japan", "Japan", "Japan", "Japan"],
            "year": ["2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023"],
            "Full-flowering date": ["404", "409", "330", "405", "401", "326", "401", "325"],
        }
    )

    tb = pr.concat([tb, tb_new])

    return tb
