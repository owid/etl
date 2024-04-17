"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Names of snapshot files to load and process.
FILES = [
    "ocean_heat_content_monthly_world_700m",
    "ocean_heat_content_monthly_world_2000m",
    "ocean_heat_content_annual_world_700m",
    "ocean_heat_content_annual_world_2000m",
]

# Columns to select from annual data, and how to rename them.
COLUMNS_ANNUAL = {
    "YEAR": "date",
    "WO": "ocean_heat_content",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load data from snapshots.
    tables_monthly = []
    tables_annual = []
    for file_name in FILES:
        # Extract depth and location from file name.
        depth = int(file_name.split("_")[-1].replace("m", ""))
        location = file_name.split("_")[-2].title()
        if "monthly" in file_name:
            # Read data.
            new_table = paths.load_snapshot(f"{file_name}.csv").read(names=["date", "ocean_heat_content"])
            # Add columns for location and depth.
            new_table = new_table.assign(**{"depth": depth, "location": location})
            # Add monthly table to list.
            tables_monthly.append(new_table)
        elif "annual" in file_name:
            # Read data, select and rename columns.
            new_table = (
                paths.load_snapshot(f"{file_name}.csv")
                .read_fwf()[list(COLUMNS_ANNUAL)]
                .rename(columns=COLUMNS_ANNUAL, errors="raise")
            )
            # Add columns for location and depth.
            new_table = new_table.assign(**{"depth": depth, "location": location})
            # Add annual table to list.
            tables_annual.append(new_table)
        else:
            raise ValueError(f"Unexpected file name: {file_name}")

    #
    # Process data.
    #
    # Combine monthly data and add a column for location.
    tb_monthly = pr.concat(tables_monthly, short_name="ocean_heat_content_monthly")

    # Combine annual data.
    tb_annual = pr.concat(tables_annual, short_name="ocean_heat_content_annual")

    # Set an appropriate index and sort conveniently.
    tb_monthly = tb_monthly.set_index(["location", "depth", "date"], verify_integrity=True).sort_index()
    tb_annual = tb_annual.set_index(["location", "depth", "date"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = create_dataset(dest_dir, tables=[tb_annual, tb_monthly], check_variables_metadata=True)
    ds_meadow.save()
