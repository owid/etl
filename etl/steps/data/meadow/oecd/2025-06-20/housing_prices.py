"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

COLUMN_NAMES = {
    "Reference area": "country",
    "Measure": "measure",
    "Unit of measure": "unit",
    "TIME_PERIOD": "year",
    "OBS_VALUE": "value",
    "BASE_PER": "base_period",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("housing_prices.csv")

    # Load data from snapshot.
    tb = snap.read()

    # Filter for annual data.
    tb = tb[tb["Frequency of observation"] == "Annual"]

    # only keep relevant columns
    tb = tb[COLUMN_NAMES.keys()]

    tb = tb.rename(columns=COLUMN_NAMES)

    # Process data.
    #
    # Improve tables format.
    tables = [tb.format(["country", "year", "measure"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
