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
    tb = snap.read()

    #
    # Process data.
    #
    # Rename the entity column to the OWID-standard "country".
    tb = tb.rename(columns={"Entity": "country"})

    # Use categoricals for the low-cardinality country column (smaller, faster).
    tb["country"] = tb["country"].astype("category")

    # Improve tables format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
