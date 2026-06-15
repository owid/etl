"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("whaling_total_catches.xlsx")

    # Load data from snapshot.
    tb = snap.read_excel(sheet_name="DB7.2", header=0)

    tb = tb.rename(columns={"Nation": "country", "Year": "year"})

    # reset index to have unique identifier for each row
    tb = tb.reset_index(drop=True).reset_index()

    #
    # Process data.
    #
    # Improve tables format.
    tables = [tb.format(["country", "year", "index"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
