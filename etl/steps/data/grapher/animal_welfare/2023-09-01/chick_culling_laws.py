"""Load a garden dataset and create a grapher dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

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


def generate_annotations_for_each_country(tb: Table):
    tb_updated = tb.copy()

    # Add annotations for each country.
    annotations = {}
    # Select countries that have some laws regarding chick culling.
    countries_with_ban = tb[tb["status"] != STATUS_NOT_BANNED]["country"].unique()
    for country in countries_with_ban:
        year_effective = tb[tb["country"] == country]["year_effective"].item()
        if year_effective <= CURRENT_YEAR:
            annotations[country] = f"{country}: Banned since {year_effective}."
        else:
            annotations[country] = f"{country}: Ban starts in {year_effective}."

    # Add any other annotation.
    countries_with_annotations = tb[~tb["annotation"].isnull()]["country"].unique()
    for country in countries_with_annotations:
        additional_annotation = tb[tb["country"] == country]["annotation"].item()
        if country in annotations:
            annotations[country] += " " + additional_annotation
        else:
            annotations[country] = f"{country}: {additional_annotation}"

    # Convert to list.
    annotations = list(annotations.values())

    # Update display metadata.
    if tb_updated["status"].metadata.display is None:
        tb_updated["status"].metadata.display = {}
    tb_updated["status"].metadata.display["annotations"] = annotations

    return tb_updated


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("chick_culling_laws")

    # Read table from garden dataset.
    tb = ds_garden["chick_culling_laws"].reset_index()

    #
    # Process data.
    #
    # Check that all status values are defined.
    assert (
        status_unknown := (set(tb["status"]) - STATUS_ALL)
    ) == set(), f"Undefined status of banning: {status_unknown}"

    # Add annotations for each country to metadata.
    # NOTE: For now annotations are not shown in map tabs. Possibly in the future they will appear in map tab tooltips.
    tb = generate_annotations_for_each_country(tb=tb)

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
