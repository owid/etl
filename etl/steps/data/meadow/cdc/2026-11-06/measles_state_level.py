"""Load the snapshots and create a meadow dataset."""

import json

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshots: finalized annual counts (CDC WONDER) and preliminary counts for recent years (CDC measles page).
    snap_annual = paths.load_snapshot("measles_state_level.csv")
    snap_recent = paths.load_snapshot("measles_cases_by_state.json")
    tb_annual = snap_annual.read()
    with open(snap_recent.path) as f:
        data_recent = json.load(f)

    #
    # Process data.
    #
    tb_annual = tb_annual.rename(columns={"Regions/States": "country"})
    tb_annual["country"] = tb_annual["country"].astype("category")
    # Keep the cases for each state and year, and the date the page was last updated.
    tb_recent = snap_recent.read_from_records(
        [
            {
                "geography": row["geography"],
                "year": int(row["year"]),
                "cases_calendar_year": row["cases_calendar_year"],
                "last_updated": data_recent["lastUpdated"],
            }
            for row in data_recent["data"]
        ]
    )

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tables = [
        tb_annual.format(["country", "year", "disease"], short_name="measles_state_level"),
        tb_recent.format(["geography", "year"], short_name="measles_cases_by_state"),
    ]

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the annual snapshot.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap_annual.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
