"""Load a snapshot and create a garden dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Map the producer's column names to the short names used since the 2024 release.
COLUMNS = {
    "Entity": "country",
    "Year": "year",
    "Share of populations increasing": "share_increasing",
    "Share of populations stable": "share_stable",
    "Share of populations decreasing": "share_decreasing",
    "Share of populations strongly increasing": "share_strong_increase",
    "Share of populations moderately increasing": "share_moderate_increase",
    "Share of populations with little change": "share_little_change",
    "Share of populations moderately decreasing": "share_moderate_decrease",
    "Share of populations strongly decreasing": "share_strong_decrease",
}
# Two alternative, mutually exclusive classifications of population trends. Each should sum to 100%.
THREE_CATEGORIES = ["share_increasing", "share_stable", "share_decreasing"]
FIVE_CATEGORIES = [
    "share_strong_increase",
    "share_moderate_increase",
    "share_little_change",
    "share_moderate_decrease",
    "share_strong_decrease",
]
# Tolerance for rounding in the published shares (one decimal place).
SUM_TOLERANCE = 0.3


def run() -> None:
    #
    # Load inputs.
    #
    snap = paths.load_snapshot("living_planet_index_share.csv")
    tb = snap.read()

    #
    # Process data.
    #
    assert set(tb.columns) == set(COLUMNS), f"Unexpected columns: {set(tb.columns) ^ set(COLUMNS)}"
    tb = tb.rename(columns=COLUMNS, errors="raise")
    sanity_check_outputs(tb)
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()


def sanity_check_outputs(tb: Table) -> None:
    shares = THREE_CATEGORIES + FIVE_CATEGORIES
    assert ((tb[shares] >= 0) & (tb[shares] <= 100) | tb[shares].isnull()).all().all(), "Shares outside [0, 100]."
    for group in [THREE_CATEGORIES, FIVE_CATEGORIES]:
        # A classification is either fully reported for an entity, or not at all.
        n_reported = tb[group].notnull().sum(axis=1)
        assert n_reported.isin([0, len(group)]).all(), f"Partially reported categories in {group}."
        totals = tb.loc[n_reported == len(group), group].sum(axis=1)
        assert ((totals - 100).abs() <= SUM_TOLERANCE).all(), f"Shares in {group} do not add up to 100%:\n{totals}"
