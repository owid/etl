"""Load the snapshots and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshots: finalized annual counts (CDC WONDER) and provisional weekly counts (NNDSS Weekly Data).
    snap_annual = paths.load_snapshot("measles_state_level.csv")
    snap_weekly = paths.load_snapshot("nndss_weekly_measles.csv")
    tb_annual = snap_annual.read()
    tb_weekly = snap_weekly.read()

    #
    # Process data.
    #
    tb_annual = tb_annual.rename(columns={"Regions/States": "country"})
    tb_annual["country"] = tb_annual["country"].astype("category")
    # Keep the cumulative year-to-date count for the current year (m3) and its flag; drop map coordinates.
    tb_weekly = tb_weekly[["states", "year", "week", "label", "m3", "m3_flag"]]
    tb_weekly["states"] = tb_weekly["states"].astype("category")

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tables = [
        tb_annual.format(["country", "year", "disease"], short_name="measles_state_level"),
        tb_weekly.format(["states", "year", "week", "label"], short_name="nndss_weekly_measles"),
    ]

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the annual snapshot.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap_annual.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
