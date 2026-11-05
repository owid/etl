"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Names of snapshot files to load and process.
FILES = [
    "sea_surface_temperature_world",
    "sea_surface_temperature_northern_hemisphere",
    "sea_surface_temperature_southern_hemisphere",
]


def run() -> None:
    #
    # Load inputs.
    #
    # Load data from each of the snapshots, and add a column with the region name.
    tables = [
        paths.load_snapshot(f"{file_name}.csv")
        .read()
        .assign(**{"location": file_name.split("sea_surface_temperature_")[-1].replace("_", " ").title()})
        for file_name in FILES
    ]

    #
    # Process data.
    #
    # Concatenate all tables.
    tb = pr.concat(tables)

    # Set an appropriate index and sort conveniently.
    tb = tb.format(["location", "year", "month"], sort_columns=True, short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb])
    ds_meadow.save()
