"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define columns and new names
COLUMNS = {"Title": "year", "CPI INDEX 00: ALL ITEMS 2015=100": "cpi"}


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("cpi_uk.csv")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    #
    # Drop rows 0 to 7 which contain metadata information
    tb = tb.drop(index=tb.index[0:7]).reset_index(drop=True)

    # Rename columns
    tb = tb.rename(columns=COLUMNS, errors="raise")

    # Keep only years that are integers
    tb = tb[tb["year"].str.isdigit()].reset_index(drop=True)

    # Add country column
    tb["country"] = "United Kingdom"

    # Improve tables format.
    tables = [tb.format(["country", "year"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
