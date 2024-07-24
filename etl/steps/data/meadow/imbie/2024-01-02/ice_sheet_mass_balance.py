"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Names of snapshot files to load and process.
FILES = [
    "ice_sheet_mass_balance_antarctica",
    "ice_sheet_mass_balance_greenland",
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load snapshot and read its data.
    tables = []
    for file_name in FILES:
        tb = paths.load_snapshot(f"{file_name}.csv").read()
        # Add a column for location.
        tb["location"] = file_name.split("ice_sheet_mass_balance_")[-1].replace("_", " ").title()
        # Add table to list.
        tables.append(tb)

    #
    # Process data.
    #
    # Combine data from all tables and ensure columns are snake-case.
    tb = pr.concat(tables).underscore()

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["location", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    # Update table name.
    tb.metadata.short_name = paths.short_name

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_meadow.save()
