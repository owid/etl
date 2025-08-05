"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("literacy_1900_1950.csv")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    #
    tb = tb[["Country", "Age", "Sex", "Year", "Percentage of illiteracy"]]

    tb = tb.rename(columns={"Percentage of illiteracy": "illiteracy_rate"})
    # Improve tables format.
    tables = [tb.format(["country", "year", "age", "sex"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
