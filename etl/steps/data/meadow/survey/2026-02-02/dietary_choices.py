"""Load snapshots and create a meadow dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Mapping from snapshot file names to country names.
SNAPSHOTS = {
    "dietary_choices_uk.xlsx": "United Kingdom",
    "dietary_choices_us.xlsx": "United States",
}


def run() -> None:
    #
    # Load inputs.
    #
    tables = []
    for snapshot_name, country in SNAPSHOTS.items():
        snap = paths.load_snapshot(snapshot_name)
        data = snap.ExcelFile()

        # Combine all sheets into a single table, adding a "group" column with the sheet name.
        tb = pr.concat(
            [data.parse(sheet_name=sheet_name).assign(**{"group": sheet_name}) for sheet_name in data.sheet_names]
        )

        # Add country column.
        tb["country"] = country

        tables.append(tb)

    # Combine UK and US data.
    tb = pr.concat(tables)

    # Improve table format.
    tb = tb.format(["which_of_these_best_describes_your_diet", "group", "country"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb])

    # Save meadow dataset.
    ds_meadow.save()
