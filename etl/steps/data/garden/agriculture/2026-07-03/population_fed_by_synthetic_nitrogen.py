"""Combine the share of the world population fed by synthetic nitrogen fertilizers (from Erisman et al. (2008)) with OWID's long-run world population, to estimate the number of people fed (and supported without) synthetic nitrogen fertilizers."""

import pandas as pd
from owid.catalog import Table
from owid.catalog import processing as pr

from etl.data_helpers.misc import interpolate_table
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Share (%) of the world population fed by synthetic nitrogen fertilizers estimated by Erisman et al. (2008) for 2008, assumed to remain constant afterwards.
# NOTE: This assumption is consistent with Rosa & Gabrielli (2023), whose country-level estimate for 2019 (about 3.8 billion people fed, roughly half of the world population) they report as consistent with the year-2000 estimates.
SHARE_FED_RECENT = 48.0

# Name of the column with the digitized share.
SHARE_COLUMN = "share_of_population_fed_by_synthetic_nitrogen"


def sanity_check_inputs(tb: Table, last_year: int) -> None:
    # Expected digitized years from Figure 1 of Erisman et al. (2008).
    expected_years = {1900, 1910, 1930, 1940, 1950, 1955, 1960, 1970, 1980, 1990, 2000, 2008}
    assert set(tb["year"]) == expected_years, "Digitized years changed unexpectedly."
    share = tb.sort_values("year")[SHARE_COLUMN]
    assert share.notna().all(), "Digitized share contains missing values."
    assert share.iloc[0] == 0, "Share in 1900 is expected to be zero."
    assert share.iloc[-1] == SHARE_FED_RECENT, (
        "Share in 2008 is expected to coincide with the share assumed for recent years."
    )
    assert last_year >= 2023, "Population estimates end earlier than expected."


def sanity_check_outputs(tb: Table) -> None:
    assert tb.columns[tb.isna().all()].empty, "Output has a fully-NaN column."
    # After interpolation, every indicator must be defined for every year (annual series, no gaps).
    assert tb.notna().all().all(), "Output has missing values after interpolation."
    assert (tb["world_population"] > 0).all(), "World population is expected to be positive."
    assert tb["year"].is_monotonic_increasing and not tb["year"].duplicated().any(), (
        "Years are not a clean annual axis."
    )
    # The interpolated share must stay within the range of the digitized endpoints.
    assert tb[SHARE_COLUMN].between(0, SHARE_FED_RECENT).all(), "Interpolated share is out of the expected range."
    # The two estimated populations must add up to the total.
    assert (
        (tb["population_fed_by_synthetic_nitrogen"] + tb["population_not_fed_by_synthetic_nitrogen"])
        == tb["world_population"]
    ).all(), "Estimated populations fed and not fed by synthetic nitrogen do not add up to the world population."
    # The share must be constant for all years after the last estimate by Erisman et al. (2008).
    recent = tb[tb["year"] >= 2008]
    assert (recent[SHARE_COLUMN] == SHARE_FED_RECENT).all(), (
        "Share of population fed is expected to be constant in recent years."
    )


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset with the digitized share of the world population fed by synthetic nitrogen fertilizers.
    ds_meadow = paths.load_dataset("population_fed_by_synthetic_nitrogen")
    tb = ds_meadow.read("population_fed_by_synthetic_nitrogen")

    # Load population dataset.
    ds_population = paths.load_dataset("population")

    #
    # Process data.
    #
    # Find the last year for which population estimates (not projections) are available, to avoid extending the series into projected future years.
    tb_historical = ds_population.read("historical")
    last_year = int(tb_historical.loc[tb_historical["country"] == "World", "year"].max())

    sanity_check_inputs(tb=tb, last_year=last_year)

    # Build an annual "World" spine from 1900 to the last year with population estimates.
    tb_spine = Table(pd.DataFrame({"country": "World", "year": range(1900, last_year + 1)}))

    # Merge the (sparse) digitized share onto the annual spine.
    tb = pr.merge(tb_spine, tb, on="year", how="left")

    # Erisman et al. (2008) estimates end in 2008; assume the share remains constant afterwards.
    tb.loc[tb["year"] > 2008, SHARE_COLUMN] = SHARE_FED_RECENT

    # The digitized share is only given for select years (1900, 1910, 1930, ...); linearly interpolate between them to obtain one value per year, so the derived populations form annual series rather than sparse points.
    tb = interpolate_table(tb, entity_col="country", time_col="year", time_mode="none", method="linear")

    # Add world population to table, properly handling origins.
    tb = paths.regions.add_population(tb, population_col="world_population")

    # Estimate the number of people fed by synthetic nitrogen fertilizers, and the number of people that could be supported without them.
    tb["population_fed_by_synthetic_nitrogen"] = (tb[SHARE_COLUMN] / 100 * tb["world_population"]).round(0)
    tb["population_not_fed_by_synthetic_nitrogen"] = tb["world_population"] - tb["population_fed_by_synthetic_nitrogen"]
    tb["population_not_fed_by_synthetic_nitrogen"].metadata.origins = tb[
        "population_fed_by_synthetic_nitrogen"
    ].metadata.origins

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
