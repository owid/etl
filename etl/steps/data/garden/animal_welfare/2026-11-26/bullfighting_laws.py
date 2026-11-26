"""Load a meadow dataset and create a garden dataset.

The snapshot gives the current status of bullfighting in each country, and the years when each ban was passed and took
effect. This step turns that into a status for each country and year, so that charts can show when each law changed.
"""

from owid.catalog import Table
from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Statuses given in the snapshot.
STATUS_BANNED = "Banned"
STATUS_PARTIALLY_BANNED = "Partially banned"
STATUS_NOT_BANNED = "Not banned"
# Status of a ban in the years between when it was passed and when it takes effect.
STATUS_BANNED_NOT_IN_EFFECT = "Banned but not yet in effect"


def sanity_check_inputs(tb: Table, last_year: int) -> None:
    error = "Unexpected status in the snapshot."
    assert set(tb["status"]) <= {STATUS_BANNED, STATUS_PARTIALLY_BANNED, STATUS_NOT_BANNED}, error
    error = "A country where bullfighting is not banned has a year when a ban took effect."
    assert tb[tb["status"] == STATUS_NOT_BANNED][["year_passed", "year_effective"]].isna().all().all(), error
    error = "A ban was passed after it took effect."
    assert (
        tb.dropna(subset=["year_passed"])["year_passed"] < tb.dropna(subset=["year_passed"])["year_effective"]
    ).all(), error
    error = "A ban takes effect after the latest year, but the year when it was passed is not given."
    assert tb[tb["year_effective"] > last_year]["year_passed"].notna().all(), error


def create_status_by_year(tb: Table, first_year: int, last_year: int) -> Table:
    # Repeat each country's row for every year.
    tb_years = Table({"year": range(first_year, last_year + 1)})
    tb = pr.merge(tb, tb_years, how="cross")

    # Before the ban (or partial ban) was passed, bullfighting was not banned.
    year_passed = tb["year_passed"].fillna(tb["year_effective"])
    before_ban = tb["year"] < year_passed
    # Between the year when a ban was passed and the year when it took effect, it was not yet in effect.
    not_in_effect = (tb["year"] >= tb["year_passed"]) & (tb["year"] < tb["year_effective"])
    # For bans with no known year, the status is only known for the latest year.
    unknown_start = tb["year_effective"].isna() & (tb["status"] != STATUS_NOT_BANNED) & (tb["year"] < last_year)

    tb.loc[before_ban, "status"] = STATUS_NOT_BANNED
    tb.loc[not_in_effect, "status"] = STATUS_BANNED_NOT_IN_EFFECT
    tb = tb[~unknown_start].reset_index(drop=True)

    return tb[["country", "year", "status"]]


def sanity_check_outputs(tb: Table, tb_snapshot: Table, last_year: int) -> None:
    # The status in the latest year must be the one given in the snapshot (unless the ban is not yet in effect).
    latest = pr.merge(tb[tb["year"] == last_year], tb_snapshot, on="country", suffixes=("", "_snapshot"))
    assert len(latest) == len(tb_snapshot), "Some countries have no status in the latest year."
    expected = latest["status_snapshot"].copy()
    expected[latest["year_effective"] > last_year] = STATUS_BANNED_NOT_IN_EFFECT
    error = "The status in the latest year differs from the one in the snapshot."
    assert (latest["status"] == expected).all(), error


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset, and read its main table.
    ds_meadow = paths.load_dataset("bullfighting_laws")
    tb_snapshot = ds_meadow.read("bullfighting_laws")

    #
    # Process data.
    #
    # Show the years from the earliest ban to the year of this version.
    first_year = int(tb_snapshot["year_effective"].min())
    last_year = int(paths.version.split("-")[0])
    sanity_check_inputs(tb_snapshot, last_year=last_year)

    # Create a status for each country and year.
    tb = create_status_by_year(tb_snapshot, first_year=first_year, last_year=last_year)
    sanity_check_outputs(tb, tb_snapshot=tb_snapshot, last_year=last_year)

    # Improve table format.
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)
    ds_garden.save()
