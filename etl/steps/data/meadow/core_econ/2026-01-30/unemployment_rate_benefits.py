"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define columns to keep
COLUMNS_TO_KEEP = ["Entity", "unemployment_rate", "replacement_rate"]


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("unemployment_rate_benefits.csv")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    #
    # Keep only relevant columns.
    tb = tb[COLUMNS_TO_KEEP]

    # Rename Entity to country.
    tb = tb.rename(columns={"Entity": "country"}, errors="raise")

    # Add year = 2020
    tb["year"] = 2020

    # Improve tables format.
    tables = [tb.format(["country", "year"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
