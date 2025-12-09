"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("fra.zip")

    # Load data from snapshot.
    with snap.open_archive():
        tb = snap.read_from_archive(filename="FRA_Years_2025_12_05.csv")
        tb_intervals = snap.read_from_archive(filename="Intervals_2025_12_05.csv")

    # Extract starting year from year range format (e.g., "2020-2025" -> 2020)
    tb_intervals["year"] = tb_intervals["year"].astype(str).str[:4].astype(int)

    # Check the years in the tables coincide, all the tb_intervals years should be in tb years\
    assert set(tb_intervals["year"]).issubset(
        set(tb["year"])
    ), "Years in intervals table not subset of main table years"
    # merge the two tables
    tb = tb.merge(
        tb_intervals,
        on=["name", "year", "regions", "iso3", "boreal", "temperate", "tropical", "subtropical"],
        how="left",
    )

    tb = tb.drop(
        columns=[
            "regions",
            "iso3",
        ]
    )
    tb = tb.rename(
        columns={
            "name": "country",
        }
    )

    # Improve tables format.
    tables = [tb.format(["country", "year"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
