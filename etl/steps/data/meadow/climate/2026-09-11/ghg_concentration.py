"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Names of snapshot files to load and process.
FILES = [
    "co2_concentration_monthly",
    "ch4_concentration_monthly",
    "n2o_concentration_monthly",
]


def run() -> None:
    #
    # Load inputs.
    #
    # Initialize dictionary to store raw tables.
    tables = {}
    for file_name in FILES:
        # Retrieve snapshot.
        snap = paths.load_snapshot(f"{file_name}.csv")

        # Load data from snapshot.
        tables[file_name] = snap.read(comment="#", na_values="-9.99")

    #
    # Process data.
    #
    for file_name, tb in tables.items():
        # Set an appropriate index and sort conveniently.
        tables[file_name] = tb.format(["year", "month"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with one table for each gas.
    ds_meadow = paths.create_dataset(tables=tables.values())
    ds_meadow.save()
