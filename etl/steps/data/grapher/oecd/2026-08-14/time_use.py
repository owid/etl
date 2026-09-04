"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Only publish countries whose one survey is from this year on.
#
# The source gives a single survey per country and never re-runs it, so a cutoff here drops countries
# rather than years: 26 of the 35 are published, over 2010-2024. What it buys is that no chart can put a
# 1999 survey beside a 2024 one — and every chart on this dataset is a cross-country comparison, so that
# mixing is the failure mode rather than an edge case. Chart 2541 is the case in point: a scatter with no
# entity selection, which plots whatever has data.
#
# The cutoff is applied HERE and not in garden, so the catalog keeps the source as the OECD publishes it —
# including India (1999) and China (2008), which are most of its coverage outside high-income Europe. It
# also keeps garden's own checks working on the full table: the China 2008 regression guard against the
# previous edition's published values, and the three age-of-reference exceptions, which are all pre-2010.
#
# `EARLIEST_SURVEY_YEAR` in export://static_viz/oecd/2026-08-14/time_use_by_country carries the same year
# for the same reason, and reads garden directly rather than this table. The two are independent on
# purpose — the chart's cutoff is a statement about that chart — but they should move together.
EARLIEST_SURVEY_YEAR = 2010


def run() -> None:
    #
    # Load inputs.
    #
    ds_garden = paths.load_dataset("time_use")

    tb = ds_garden.read("time_use")
    tb_groups = ds_garden.read("time_use_chart_groups")

    #
    # Process data.
    #
    # The survey-year and reference-age labels are strings for context, not indicators.
    tb = tb.drop(columns=["survey_year", "age_of_reference"])

    dropped = sorted(
        {(str(country), int(year)) for country, year in zip(tb["country"], tb["year"]) if year < EARLIEST_SURVEY_YEAR}
    )
    paths.log.info(
        f"Surveys before {EARLIEST_SURVEY_YEAR} not published: "
        + ", ".join(f"{country} ({year})" for country, year in dropped)
    )
    tb = tb[tb["year"] >= EARLIEST_SURVEY_YEAR]
    tb_groups = tb_groups[tb_groups["year"] >= EARLIEST_SURVEY_YEAR]

    # Both tables carry one row per country and sex, so they must agree on which countries survive.
    countries = set(tb["country"])
    assert countries == set(tb_groups["country"]), "The two tables no longer publish the same countries."
    assert len(countries) == 26, f"Expected 26 countries from {EARLIEST_SURVEY_YEAR} on, got {len(countries)}."
    assert tb["year"].min() >= EARLIEST_SURVEY_YEAR, "A survey older than the cutoff survived the filter."

    tb = tb.format(["country", "year", "sex"], short_name="time_use")
    tb_groups = tb_groups.format(["country", "year", "sex"], short_name="time_use_chart_groups")

    #
    # Save outputs.
    #
    ds_grapher = paths.create_dataset(
        tables=[tb, tb_groups], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )
    ds_grapher.save()
