"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("electric_car_sales.csv")

    # Load data from snapshot.
    tb = snap.read_csv()

    #
    # Process data.
    #
    # Rename the entity column to the standard "country" name.
    tb = tb.rename(columns={"Entity": "country"}, errors="raise")

    # Use categoricals for the low-cardinality country column.
    tb["country"] = tb["country"].astype("category")

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
