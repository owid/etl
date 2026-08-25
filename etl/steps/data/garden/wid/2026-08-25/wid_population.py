"""WID population counts (adults aged 20+ and total), per country and year.

WID's income series are expressed per adult ("equal-split adults"), so these counts are what
converts them to a per-capita basis and what weights countries consistently in cross-source
comparisons (see garden/poverty_inequality/.../harmonized_income_distributions).
"""

from owid.catalog import Table

from etl.helpers import PathFinder

paths = PathFinder(__file__)

log = paths.log


def run() -> None:
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("wid_population")
    tb = ds_meadow.read("wid_population")

    #
    # Process data.
    #
    # A few historical rows report adults without a total population; they cannot serve either
    # purpose of this dataset (per-capita conversion, population weighting), so drop them.
    incomplete = tb["adult_population"].isna() | tb["total_population"].isna()
    if incomplete.any():
        log.info(f"Dropping {int(incomplete.sum())} rows without both population counts.")
        tb = tb.loc[~incomplete].reset_index(drop=True)

    sanity_check_outputs(tb)

    tb = tb.format(["country", "year"], short_name="wid_population")

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()


def sanity_check_outputs(tb: Table) -> None:
    assert (tb["adult_population"] > 0).all() and (tb["total_population"] > 0).all(), (
        "Non-positive population counts found."
    )
    assert (tb["adult_population"] <= tb["total_population"]).all(), (
        "Adult population exceeds total population somewhere."
    )
    world_latest = tb.loc[(tb["country"] == "World") & (tb["year"] == 2023), "total_population"]
    assert len(world_latest) == 1 and 7.8e9 < world_latest.iloc[0] < 8.4e9, (
        f"World total population in 2023 is implausible: {world_latest.tolist()}"
    )
