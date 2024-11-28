"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Names of snapshot files to load and process.
FILES = [
    "snow_cover_extent_north_america",
    "snow_cover_extent_northern_hemisphere",
]

# Names of columns in the data.
COLUMNS = ["year", "month", "snow_cover_extent"]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load snapshot and read its data.
    tables = []
    for file_name in FILES:
        tb = paths.load_snapshot(f"{file_name}.csv").read_fwf(names=COLUMNS)
        # Add a column for location.
        tb["location"] = file_name.split("snow_cover_extent_")[-1].replace("_", " ").title()
        # Add table to list.
        tables.append(tb)

    #
    # Process data.
    #
    # Combine data from all tables.
    tb = pr.concat(tables)

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["location", "year", "month"], verify_integrity=True).sort_index().sort_index(axis=1)

    # Update table name.
    tb.metadata.short_name = paths.short_name

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_meadow.save()
