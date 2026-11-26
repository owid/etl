"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Statuses given in the snapshot.
STATUS_BANNED = "Banned"
STATUS_BANNED_NOT_IN_EFFECT = "Banned but not yet in effect"
STATUS_PARTIALLY_BANNED = "Partially banned"
STATUS_NOT_BANNED = "Not banned"


def sanity_check_inputs(tb: Table) -> None:
    error = "Unexpected status in the snapshot."
    assert set(tb["status"]) <= {
        STATUS_BANNED,
        STATUS_BANNED_NOT_IN_EFFECT,
        STATUS_PARTIALLY_BANNED,
        STATUS_NOT_BANNED,
    }, error
    error = "A country where bullfighting is not banned has a year when a ban took effect."
    assert tb[tb["status"] == STATUS_NOT_BANNED]["year_effective"].isna().all(), error
    current_year = int(paths.version.split("-")[0])
    not_in_effect = tb["status"] == STATUS_BANNED_NOT_IN_EFFECT
    error = "A ban that is not yet in effect should take effect after the current year (and the others, before it)."
    assert (tb[not_in_effect]["year_effective"] > current_year).all(), error
    assert (tb[~not_in_effect]["year_effective"].dropna() <= current_year).all(), error


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset, and read its main table.
    ds_meadow = paths.load_dataset("bullfighting_laws")
    tb = ds_meadow.read("bullfighting_laws")

    #
    # Process data.
    #
    sanity_check_inputs(tb)

    # Improve table format.
    tb = tb.format(["country"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)
    ds_garden.save()
