"""Emigration from Norway to overseas countries, 1821-1950, as a share of the population.

The series stops in 1950: from 1951 onward, Statistics Norway counted all emigration rather
than only emigration to overseas countries, so later years are not comparable.
"""

from owid.catalog import Table

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def sanity_check(tb: Table) -> None:
    assert not tb["year"].duplicated().any(), "Duplicate years."
    share = tb.set_index("year")["overseas_emigration_share"]
    # The all-time peak was in 1882, during the great overseas emigration wave.
    assert share.idxmax() == 1882, "Peak emigration share is no longer in 1882."
    assert 1.4 < share.loc[1882] < 1.6, "The 1882 peak is off (expected about 1.5% of the population)."
    assert (share >= 0).all() and (share < 2).all(), "Share outside the plausible range."


def run() -> None:
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("norway_emigration")
    tb = ds_meadow.read("norway_emigration")

    #
    # Process data.
    #
    # Keep the overseas-only era: from 1951 onward the source counts all emigration instead.
    tb = tb[tb["year"] <= 1950].copy()
    tb["country"] = "Norway"

    # The population (per 1 January) comes from the same source table.
    tb["overseas_emigration_share"] = tb["emigration"] / tb["population"] * 100
    tb = tb.drop(columns=["population", "emigration"])

    sanity_check(tb)

    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)
    ds_garden.save()
