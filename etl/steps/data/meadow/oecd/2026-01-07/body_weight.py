"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("body_weight.csv")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    #
    tb = tb[
        ["Reference area", "Measure", "Age", "Sex", "Unit of measure", "TIME_PERIOD", "OBS_VALUE", "Measurement method"]
    ]
    tb = tb.rename(columns={"Reference area": "country", "TIME_PERIOD": "year"})
    # Use only measured data (excluding self reported)
    tb = tb[tb["Measurement method"] == "Measured"]
    # Improve tables format.
    tables = [tb.format(["country", "year", "measure", "age", "sex"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
