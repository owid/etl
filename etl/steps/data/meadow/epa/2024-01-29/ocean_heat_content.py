"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Names of snapshot files to load and process.
FILES = [
    "ocean_heat_content_annual_world_700m",
    "ocean_heat_content_annual_world_2000m",
]

# Columns to select from data, and how to rename them.
COLUMNS_OCEAN_HEAT = {
    "Year": "year",
    # Data available for both 700m and 2000m.
    "IAP": "ocean_heat_content_iap",
    "MRI/JMA": "ocean_heat_content_mri",
    "NOAA": "ocean_heat_content_noaa",
    # Only available for 700m.
    "CSIRO": "ocean_heat_content_csiro",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load data from snapshots.
    tables = []
    for file_name in FILES:
        # Extract depth and location from file name.
        depth = int(file_name.split("_")[-1].replace("m", ""))
        location = file_name.split("_")[-2].title()
        # Read data, select and rename columns.
        new_table = (
            paths.load_snapshot(f"{file_name}.csv")
            .read(skiprows=6, encoding_errors="ignore")
            .rename(columns=COLUMNS_OCEAN_HEAT, errors="ignore")
        )
        # Add columns for location and depth.
        new_table = new_table.assign(**{"depth": depth, "location": location})
        # Add annual table to list.
        tables.append(new_table)

    #
    # Process data.
    #
    # Combine data.
    tb = pr.concat(tables)

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["location", "depth", "year"], verify_integrity=True).sort_index()

    # Rename tables.
    tb.metadata.short_name = "ocean_heat_content"

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_meadow.save()
