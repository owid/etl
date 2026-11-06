"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    # Step 1: Load the snapshot.
    snap = paths.load_snapshot("fish_catch.csv")
    tb = snap.read()

    # Step 2: Keep the area name as country (it is unique across the global and fishing-entity areas).
    tb = tb.rename(columns={"area_name": "country"}, errors="raise").drop(columns=["area_type", "area_id"])
    for column in ["country", "dimension", "category"]:
        tb[column] = tb[column].astype("category")

    tb = tb.format(["country", "year", "dimension", "category"])

    # Step 3: Save the meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)
    ds_meadow.save()
