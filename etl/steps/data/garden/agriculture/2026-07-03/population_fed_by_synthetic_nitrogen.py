"""Combine the share of the world population fed by synthetic nitrogen fertilizers (digitized from Figure 1 of
Erisman et al. (2008)) with OWID's long-run world population, to estimate the number of people fed (and supported
without) synthetic nitrogen fertilizers.

Erisman et al. (2008) estimated the share up to 2008 (48%). From then on, we assume the share stays constant, so the
extension of the series to recent years is driven purely by population growth. This assumption is supported by
Rosa & Gabrielli (2023, https://doi.org/10.1088/1748-9326/aca815), whose country-level estimate for 2019 (about
3.8 billion people fed by synthetic nitrogen fertilizers, roughly half of the world population) is, in the authors'
own words, consistent with the earlier estimates for the year 2000.
"""

from owid.catalog import Table
from owid.catalog import processing as pr
from structlog import get_logger

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

log = get_logger()

# First year of the output series (first year of the digitized curve in Figure 1 of Erisman et al. (2008)).
YEAR_MIN = 1900

# Last year of the estimates by Erisman et al. (2008).
YEAR_LAST_ERISMAN = 2008

# Share (%) of the world population fed by synthetic nitrogen fertilizers estimated by Erisman et al. (2008) for 2008,
# assumed to remain constant afterwards.
# NOTE: This assumption is consistent with Rosa & Gabrielli (2023), whose country-level estimate for 2019 (about
# 3.8 billion people fed, roughly half of the world population) they report as consistent with the year-2000 estimates.
SHARE_FED_RECENT = 48.0


def sanity_check_inputs(tb: Table, tb_population: Table) -> None:
    # Expected digitized years and shares from Figure 1 of Erisman et al. (2008).
    expected_years = {1900, 1910, 1930, 1940, 1950, 1955, 1960, 1970, 1980, 1990, 2000, 2008}
    assert set(tb["year"]) == expected_years, "Digitized years changed unexpectedly."
    share = tb.sort_values("year")["share_of_population_fed_by_synthetic_nitrogen"]
    assert share.notna().all(), "Digitized share contains missing values."
    assert (share.diff().dropna() >= 0).all(), "Digitized share is expected to be monotonically non-decreasing."
    assert share.iloc[0] == 0, "Share in 1900 is expected to be zero."
    assert share.iloc[-1] == SHARE_FED_RECENT, (
        "Share in 2008 is expected to coincide with the share assumed for recent years."
    )

    # World population must be annual and positive over the full output period.
    years_population = tb_population[tb_population["year"] >= YEAR_MIN]["year"]
    assert set(years_population) == set(range(YEAR_MIN, years_population.max() + 1)), (
        "World population is expected to be annual since 1900."
    )
    assert (tb_population["population"] > 0).all(), "World population is expected to be positive."
    assert years_population.max() >= 2023, "World population series ends earlier than expected."


def sanity_check_outputs(tb: Table) -> None:
    assert tb.columns[tb.isna().all()].empty, "Output has a fully-NaN column."
    assert (tb["world_population"] > 0).all(), "World population is expected to be positive."
    # Where the share is defined, the two estimated populations must add up to the total.
    defined = tb.dropna(subset=["share_of_population_fed_by_synthetic_nitrogen"])
    assert (
        (defined["population_fed_by_synthetic_nitrogen"] + defined["population_not_fed_by_synthetic_nitrogen"])
        == defined["world_population"]
    ).all(), "Estimated populations fed and not fed by synthetic nitrogen do not add up to the world population."
    # The share must be defined for all years after the last estimate by Erisman et al. (2008).
    recent = tb[tb["year"] >= YEAR_LAST_ERISMAN]
    assert (recent["share_of_population_fed_by_synthetic_nitrogen"] == SHARE_FED_RECENT).all(), (
        "Share of population fed is expected to be constant in recent years."
    )


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset with the digitized share of the world population fed by synthetic nitrogen fertilizers.
    ds_meadow = paths.load_dataset("population_fed_by_synthetic_nitrogen")
    tb = ds_meadow.read("population_fed_by_synthetic_nitrogen")

    # Load population dataset and read the table of population estimates (without projections).
    ds_population = paths.load_dataset("population")
    tb_population = ds_population.read("historical")

    #
    # Process data.
    #
    # Select world population estimates for the relevant years.
    tb_population = tb_population[
        (tb_population["country"] == "World") & (tb_population["year"] >= YEAR_MIN)
    ].reset_index(drop=True)
    tb_population = tb_population[["country", "year", "population_historical"]].rename(
        columns={"population_historical": "population"}, errors="raise"
    )

    sanity_check_inputs(tb=tb, tb_population=tb_population)

    # Combine the (sparse) digitized share with the (annual) world population.
    tb = pr.merge(tb_population, tb, on="year", how="left")

    # Erisman et al. (2008) estimates end in 2008; assume the share remains constant afterwards.
    tb.loc[tb["year"] > YEAR_LAST_ERISMAN, "share_of_population_fed_by_synthetic_nitrogen"] = SHARE_FED_RECENT

    # Estimate the number of people fed by synthetic nitrogen fertilizers, and the number of people that could be
    # supported without them.
    tb["population_fed_by_synthetic_nitrogen"] = (
        tb["share_of_population_fed_by_synthetic_nitrogen"] / 100 * tb["population"]
    ).round(0)
    tb["population_not_fed_by_synthetic_nitrogen"] = tb["population"] - tb["population_fed_by_synthetic_nitrogen"]

    # Rename the total population column for clarity (the only entity in this dataset is the World).
    tb = tb.rename(columns={"population": "world_population"}, errors="raise")

    sanity_check_outputs(tb=tb)

    # Improve table format.
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb])

    # Save changes in the new garden dataset.
    ds_garden.save()
