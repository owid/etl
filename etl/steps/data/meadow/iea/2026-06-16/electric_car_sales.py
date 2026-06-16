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
    tb = snap.read()

    #
    # Process data.
    #
    # Rename the entity column to country.
    tb = tb.rename(columns={"Entity": "country"}, errors="raise")

    # Cast low-cardinality string column to category for performance.
    tb["country"] = tb["country"].astype("category")

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)
    ds_meadow.save()
