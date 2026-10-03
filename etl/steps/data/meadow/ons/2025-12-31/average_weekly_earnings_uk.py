"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define columns and new names
COLUMNS = {"Unnamed: 0": "date", "Unnamed: 1": "average_weekly_earnings"}


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("average_weekly_earnings_uk.xlsx")

    # Load data from snapshot.
    tb = snap.read(sheet_name="AWE Real_CPI", skiprows=7, usecols="A:B")

    #
    # Process data.
    #
    # Rename columns
    tb = tb.rename(columns=COLUMNS, errors="raise")

    # Only keep columns where average_weekly_earnings is not null
    tb = tb[tb["average_weekly_earnings"].notnull()].reset_index(drop=True)

    # Improve tables format.
    tables = [tb.format(["date"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
