"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Current year.
CURRENT_YEAR = int(paths.version.split("-")[0])

# Define available status names (they should coincide with those used in snapshot).
STATUS_BANNED = "Banned"
STATUS_BANNED_NOT_EFFECTIVE = "Banned but not yet effective"
STATUS_BANNED_PARTIALLY = "Partially banned"
STATUS_NOT_BANNED = "Not banned"
STATUS_ALL = {STATUS_BANNED, STATUS_BANNED_NOT_EFFECTIVE, STATUS_BANNED_PARTIALLY, STATUS_NOT_BANNED}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("bullfighting_laws")

    # Read table from garden dataset.
    tb = ds_garden["bullfighting_laws"].reset_index()

    #
    # Process data.
    #
    # Check that all status values are defined.
    assert (
        status_unknown := (set(tb["status"]) - STATUS_ALL)
    ) == set(), f"Undefined status of banning: {status_unknown}"

    # Add annotations for each country to metadata.
    # NOTE: For now annotations are not shown in map tabs. Possibly in the future they will appear in map tab tooltips.
    # If so, copy the function generate_annotations_for_each_country() from the chick_culling_laws grapher step.
    # tb = generate_annotations_for_each_country(tb=tb)

    # Select relevant columns.
    tb = tb[["country", "status"]]

    # Add current year (there should be no timeline in the chart).
    tb["year"] = CURRENT_YEAR

    # Set an appropriate index.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)

    # Save changes in the new grapher dataset.
    ds_grapher.save()
