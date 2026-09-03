"""Load a garden dataset and create a grapher dataset.

Each country's effective year is embedded in the status value (e.g. "Banned (2023)") so that the map
tooltip shows it. The chart and explorer steps map each status-year value back to its plain status.
"""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Current year.
CURRENT_YEAR = int(paths.version.split("-")[0])

# Define available status names (they should coincide with those used in snapshot).
STATUS_BANNED = "Banned"
STATUS_BANNED_NOT_EFFECTIVE = "Banned but not yet in effect"
STATUS_BANNED_PARTIALLY = "Partially banned"
STATUS_NOT_BANNED = "Not banned"
STATUS_ALL = {STATUS_BANNED, STATUS_BANNED_NOT_EFFECTIVE, STATUS_BANNED_PARTIALLY, STATUS_NOT_BANNED}


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("chick_culling_laws")

    # Read table from garden dataset.
    tb = ds_garden.read("chick_culling_laws")

    #
    # Process data.
    #
    # Check that all status values are defined.
    assert (status_unknown := (set(tb["status"]) - STATUS_ALL)) == set(), (
        f"Undefined status of banning: {status_unknown}"
    )

    # Embed each country's effective year in the status value so it shows in the map tooltip.
    status = tb["status"]
    is_banned = status != STATUS_NOT_BANNED
    with_year = status.astype("string") + " (" + tb["year_effective"].astype("Int64").astype("string") + ")"
    new_status = status.astype("string")
    new_status[is_banned] = with_year[is_banned]
    tb["status"] = new_status.copy_metadata(status)

    # Select relevant columns.
    tb = tb[["country", "status"]]

    # Add current year (there should be no timeline in the chart).
    tb["year"] = CURRENT_YEAR

    # Improve table format.
    tb = tb.format()

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = paths.create_dataset(tables=[tb])

    # Save changes in the new grapher dataset.
    ds_grapher.save()
