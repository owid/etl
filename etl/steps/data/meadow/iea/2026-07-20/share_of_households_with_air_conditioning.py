"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("share_of_households_with_air_conditioning.csv")

    # Load data from snapshot.
    tb = snap.read_csv()

    #
    # Process data.
    #
    # Rename the entity column to the standard "country" column.
    tb = tb.rename(columns={"entity": "country"})

    # Use categorical dtype for the low-cardinality country column.
    tb["country"] = tb["country"].astype("category")

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)
    ds_meadow.save()
