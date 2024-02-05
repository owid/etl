"""Load a snapshot and create a meadow dataset."""


from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Names of snapshot files to load and process.
FILES = [
    "co2_concentration_monthly",
    "ch4_concentration_monthly",
    "n2o_concentration_monthly",
]


def run(dest_dir: str) -> None:
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
        tables[file_name] = tb.set_index(["year", "month"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset with one table for each gas.
    ds_meadow = create_dataset(dest_dir, tables=tables.values(), check_variables_metadata=True)
    ds_meadow.save()
