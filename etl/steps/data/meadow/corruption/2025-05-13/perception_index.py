"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("perception_index.csv")

    tb = snap.read(encoding="ISO-8859-1", skiprows=2)  # You can also try "latin1" if this doesn't work
    #
    # Process data.
    #
    columns = ["Country / Territory", "Year", "CPI score", "Lower CI", "Upper CI"]
    tb = tb[columns]
    tb = tb.rename(columns={"Country / Territory": "country"})
    tables = [tb.format(["country", "year"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
