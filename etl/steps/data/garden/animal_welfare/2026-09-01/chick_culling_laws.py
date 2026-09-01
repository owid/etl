"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Get current year from this step's version.
CURRENT_YEAR = int(paths.version.split("-")[0])

# Define available status names (they should coincide with those used in the snapshot).
STATUS_BANNED = "Banned"
STATUS_BANNED_NOT_EFFECTIVE = "Banned but not yet in effect"
STATUS_BANNED_PARTIALLY = "Partially banned"
STATUS_NOT_BANNED = "Not banned"
STATUS_ALL = {STATUS_BANNED, STATUS_BANNED_NOT_EFFECTIVE, STATUS_BANNED_PARTIALLY, STATUS_NOT_BANNED}


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("chick_culling_laws")
    tb = ds_meadow.read("chick_culling_laws")

    # Load regions dataset and read its main table.
    ds_regions = paths.load_dataset("regions")
    tb_regions = ds_regions.read("regions")

    #
    # Process data.
    #
    # Run sanity checks on inputs.
    sanity_check_inputs(tb=tb)

    # Add all countries that are not in the data, assuming chick culling is not banned there.
    # NOTE: Chick culling bans are notable legislative events that animal advocacy organizations track closely, so a
    # country with no known law can safely be assumed to have no ban (instead of showing it as having no data).
    tb_added = (
        tb_regions[
            (~tb_regions["name"].isin(tb["country"].unique()))
            & (tb_regions["region_type"] == "country")
            & (~tb_regions["is_historical"])
            & (tb_regions["defined_by"] == "owid")
        ][["name"]]
        .assign(**{"status": STATUS_NOT_BANNED})
        .rename(columns={"name": "country"}, errors="raise")
    )
    tb = pr.concat([tb, tb_added], ignore_index=True)

    # Set an appropriate index and sort conveniently.
    tb = tb.format(keys=["country"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb])

    # Save changes in the new garden dataset.
    ds_garden.save()


def sanity_check_inputs(tb: Table) -> None:
    error = f"Undefined status of banning: {set(tb['status']) - STATUS_ALL}"
    assert set(tb["status"]) <= STATUS_ALL, error

    error = "Duplicated countries found in the snapshot data."
    assert not tb["country"].duplicated().any(), error

    error = "All banned statuses should have a year when the ban became (or will become) effective."
    assert tb[tb["status"] != STATUS_NOT_BANNED]["year_effective"].notna().all(), error

    error = (
        "A ban marked as not yet in effect has an effective year in the past. "
        "Check whether it is now in effect and update the snapshot accordingly."
    )
    assert (tb[tb["status"] == STATUS_BANNED_NOT_EFFECTIVE]["year_effective"] >= CURRENT_YEAR).all(), error
