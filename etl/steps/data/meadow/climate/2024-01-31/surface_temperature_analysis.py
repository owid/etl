"""Load a snapshot and create a meadow dataset."""


from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Names of snapshot files to load and process.
FILES = [
    "surface_temperature_analysis_world",
    "surface_temperature_analysis_northern_hemisphere",
    "surface_temperature_analysis_southern_hemisphere",
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
        tables[file_name] = snap.read(
            skiprows=1,
            na_values="***",
            usecols=[
                "Year",
                "Jan",
                "Feb",
                "Mar",
                "Apr",
                "May",
                "Jun",
                "Jul",
                "Aug",
                "Sep",
                "Oct",
                "Nov",
                "Dec",
            ],
        )

    #
    # Process data.
    #
    for file_name, tb in tables.items():
        # Set an appropriate index and sort conveniently.
        tables[file_name] = tb.set_index(["Year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=tables.values(), check_variables_metadata=True)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
