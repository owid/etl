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
        tb_int = snap.read_from_archive(filename="Intervals_2025_12_05.csv")

    # take first 4 digits of year range to get starting year
    tb_int["year"] = tb_int["year"].astype(str).str[:4].astype(int)

    # merge the two tables
    tb = tb.merge(
        tb_int, on=["name", "year", "regions", "iso3", "boreal", "temperate", "tropical", "subtropical"], how="outer"
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
    # Process data.
    #
    # Improve tables format.
    tables = [tb.format(["country", "year"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
