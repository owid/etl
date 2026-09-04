"""Harmonize the OECD Time Use Database and build the activity groups used in OWID charts.

The source reports average minutes per day in a hierarchy of activities: five top-level categories
(paid work or study, unpaid work, personal care, leisure, other) with detailed sub-activities.
Some countries leave part of a top-level category unallocated to any sub-activity (e.g. Japan and
Poland within unpaid work), so groups that mean "the rest of a category" are computed as
remainders from the top-level totals rather than by summing sub-activities.
"""

from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder

paths = PathFinder(__file__)

log = get_logger()

# Activity codes in the source workbook, and the column name each becomes.
CODE_TO_COLUMN = {
    "1": "paid_work_or_study",
    "1.1": "paid_work_all_jobs",
    "1.2": "travel_to_and_from_work_or_study",
    "1.3": "time_in_school_or_classes",
    "1.4": "research_and_homework",
    "1.5": "job_search",
    "1.6": "other_paid_work_or_study_related",
    "2": "unpaid_work",
    "2.1": "routine_housework",
    "2.2": "shopping",
    "2.3": "care_for_household_members",
    "2.3.1": "child_care",
    "2.3.2": "adult_care",
    "2.4": "care_for_non_household_members",
    "2.5": "volunteering",
    "2.6": "travel_related_to_household_activities",
    "2.7": "other_unpaid_work_activities",
    "3": "personal_care",
    "3.1": "sleeping",
    "3.2": "eating_and_drinking",
    "3.3": "other_personal_care_services",
    "4": "leisure",
    "4.1": "sports",
    "4.2": "participating_in_or_attending_events",
    "4.3": "visiting_or_entertaining_friends",
    "4.4": "tv_or_radio_at_home",
    "4.5": "other_leisure_activities",
    "5": "other_activities",
    "5.1": "religious_spiritual_and_civic_activities",
    "5.2": "other_uncategorized_activities",
    "T": "total",
}

# Codes the chart groups draw on directly; none of these may be missing for any country/sex.
# (1.4 research/homework is genuinely unreported by a few countries and is treated as zero.)
REQUIRED_CODES = ["1", "2", "3", "4", "5", "T", "1.3", "2.1", "2.2", "3.1", "3.2", "4.3", "4.4"]

# Minutes of slack tolerated when the source's numbers are reconciled against each other.
TOLERANCE = 0.5

# Only publish countries whose one survey is from this year on.
#
# The source gives a single survey per country and never re-runs it, so a cutoff drops countries rather
# than years: 26 of the 35 survive, over 2010-2024. What it buys is that nothing downstream can put a
# 1999 survey beside a 2024 one, and every chart and every static viz on this dataset is a
# cross-country comparison, so that mixing is the failure mode rather than an edge case.
#
# What it costs is India (1999) and China (2008), and with them most of the coverage outside
# high-income Europe. Weighed against that: 2010 is where the cutoff costs least, since six countries
# sit on 2010 itself and it buys eleven years of recency for nine others; and survey year does not tilt
# what the data says, the correlation between a country's survey year and any of the four top-level
# categories being +0.09 at most (ai/time_use_comparability).
#
# It also removes every age-of-reference exception — Australia (15+), China (15-74) and Lithuania
# (20-64) are all pre-2010 — so what is published is 15-to-64 throughout, which `sanity_check_cutoff`
# asserts. Set to None to publish all 35.
EARLIEST_SURVEY_YEAR = 2010


def run() -> None:
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("time_use")
    tb = ds_meadow.read("time_use")

    sanity_check_inputs(tb)

    #
    # Process data.
    #
    tb = paths.regions.harmonize_names(tb, country_col="country", countries_file=paths.country_mapping_path)

    # The dataset has no time dimension of its own: each country reports one survey, whose year(s)
    # the source states as a label like "2009/10". Use the survey's end year as the year.
    tb["year"] = tb["survey_year"].map(parse_survey_end_year).copy_metadata(tb["minutes"])

    tb_wide = pivot_activities(tb)
    tb_groups = build_chart_groups(tb_wide)

    sanity_check_outputs(tb_wide, tb_groups)

    # The source's own total row (~1440 everywhere by construction) is only needed for the checks.
    tb_wide = tb_wide.drop(columns=["total"])

    # Applied here, after the checks: they reconcile what the OECD publishes, and one of them
    # reproduces the previous edition's published values for China 2008, which the cutoff removes.
    tb_wide, tb_groups = apply_survey_year_cutoff(tb_wide, tb_groups)

    tb_wide = tb_wide.format(["country", "year", "sex"], short_name="time_use")
    tb_groups = tb_groups.format(["country", "year", "sex"], short_name="time_use_chart_groups")

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb_wide, tb_groups], default_metadata=ds_meadow.metadata)
    ds_garden.save()


def apply_survey_year_cutoff(tb_wide: Table, tb_groups: Table) -> tuple[Table, Table]:
    """Keep only the countries whose survey is from `EARLIEST_SURVEY_YEAR` on."""
    if EARLIEST_SURVEY_YEAR is None:
        return tb_wide, tb_groups

    dropped = sorted(
        {
            (str(country), int(year))
            for country, year in zip(tb_wide["country"], tb_wide["year"])
            if year < EARLIEST_SURVEY_YEAR
        }
    )
    log.info(
        f"Surveys before {EARLIEST_SURVEY_YEAR} not published: "
        + ", ".join(f"{country} ({year})" for country, year in dropped)
    )
    tb_wide = tb_wide[tb_wide["year"] >= EARLIEST_SURVEY_YEAR]
    tb_groups = tb_groups[tb_groups["year"] >= EARLIEST_SURVEY_YEAR]
    sanity_check_cutoff(tb_wide, tb_groups)
    return tb_wide, tb_groups


def parse_survey_end_year(survey_year: str) -> int:
    """Return the end year of a survey-year label like "2016", "2009/10" or "2013/2014"."""
    start, _, end = survey_year.partition("/")
    if not end:
        return int(start)
    if len(end) == 4:
        return int(end)
    return int(start[: 4 - len(end)] + end)


def pivot_activities(tb: Table) -> Table:
    """Pivot the long table into one column per activity, keyed by country, year and sex."""
    tb_wide = tb.pivot(
        index=["country", "year", "sex", "survey_year", "age_of_reference"],
        columns="activity_code",
        values="minutes",
    )
    tb_wide = tb_wide.rename(columns=CODE_TO_COLUMN)
    return tb_wide.reset_index()


def build_chart_groups(tb_wide: Table) -> Table:
    """Group activities the way OWID's "How do people spend their time?" chart displays them.

    The grouping was reverse-engineered numerically from the previous edition of the chart and
    reproduces its published values for countries whose survey has not changed since. "The rest of
    a category" groups are remainders from the top-level totals, so minutes a country leaves
    unallocated to any sub-activity stay in the group and every row still sums to 24 hours.
    """
    tb = tb_wide[["country", "year", "sex"]].copy()

    education = tb_wide["time_in_school_or_classes"] + tb_wide["research_and_homework"].fillna(0)
    tb["paid_work"] = tb_wide["paid_work_or_study"] - education
    tb["education"] = education
    tb["sleep"] = tb_wide["sleeping"]
    tb["housework_and_shopping"] = tb_wide["routine_housework"] + tb_wide["shopping"]
    # Care work, volunteering and household travel, plus the source's small "other" category
    # (religious/spiritual activities, civic obligations and uncategorized time).
    tb["other_unpaid_work"] = tb_wide["unpaid_work"] - tb["housework_and_shopping"] + tb_wide["other_activities"]
    tb["eating_and_drinking"] = tb_wide["eating_and_drinking"]
    tb["personal_care"] = tb_wide["personal_care"] - tb_wide["sleeping"] - tb_wide["eating_and_drinking"]
    tb["tv_and_radio"] = tb_wide["tv_or_radio_at_home"]
    tb["seeing_friends"] = tb_wide["visiting_or_entertaining_friends"]
    tb["other_leisure"] = (
        tb_wide["leisure"] - tb_wide["tv_or_radio_at_home"] - tb_wide["visiting_or_entertaining_friends"]
    )
    tb["total_leisure"] = tb_wide["leisure"]

    return tb


def sanity_check_cutoff(tb_wide: Table, tb_groups: Table) -> None:
    """Check what the cutoff left: the same countries in both tables, and one reference age."""
    countries = set(tb_wide["country"])
    assert countries == set(tb_groups["country"]), "The two tables no longer cover the same countries."
    expected = 26 if EARLIEST_SURVEY_YEAR == 2010 else None
    assert expected is None or len(countries) == expected, (
        f"Expected {expected} countries from {EARLIEST_SURVEY_YEAR} on, got {len(countries)}."
    )
    assert tb_wide["year"].min() >= EARLIEST_SURVEY_YEAR, "A survey older than the cutoff survived."
    # Every age-of-reference exception the source flags is pre-2010, so a 2010 cutoff leaves one age
    # range — which is what lets everything downstream say "aged 15 to 64" without qualifying it.
    ages = set(tb_wide["age_of_reference"].astype(str))
    assert ages == {"15-64"}, f"More than one reference age survived the cutoff: {sorted(ages)}."


def sanity_check_inputs(tb: Table) -> None:
    assert set(tb["activity_code"].unique()) == set(CODE_TO_COLUMN), "Activity codes changed at the source."
    assert not tb.duplicated(subset=["country", "sex", "activity_code"]).any(), "Duplicate activity rows."
    assert tb["country"].nunique() >= 35, "Country coverage shrank against the previous release."
    assert set(tb["sex"].unique()) == {"total", "men", "women"}, "Unexpected sex categories."
    assert tb["minutes"].min() >= 0, "Negative minutes found."

    # One survey per country: the three sheets must agree on its year and reference ages.
    per_country = tb.groupby("country", observed=True)[["survey_year", "age_of_reference"]].nunique()
    assert (per_country == 1).all().all(), "Survey year or age of reference differs across sheets."

    required = tb[tb["activity_code"].isin(REQUIRED_CODES)]
    missing = required[required["minutes"].isna()]
    assert missing.empty, f"Missing values in required activities:\n{missing}"

    # The source normalizes each country to 1440 minutes per day.
    totals = tb[tb["activity_code"] == "T"]["minutes"]
    assert ((totals - 1440).abs() < TOLERANCE).all(), "Source total deviates from 1440 minutes per day."


def sanity_check_outputs(tb_wide: Table, tb_groups: Table) -> None:
    group_columns = [
        column for column in tb_groups.columns if column not in ["country", "year", "sex", "total_leisure"]
    ]

    assert tb_groups[group_columns].notna().all().all(), "A chart group has missing values."
    assert (tb_groups[group_columns] >= 0).all().all(), "A chart group is negative."

    # The groups partition the day: they must add back up to the source's normalized total.
    # NOTE: the source's own top-level categories deviate from its total row by up to ~1.2 minutes
    # for Japan and New Zealand (women) — rounding on the source's side — hence the 2-minute bar.
    group_sums = tb_groups[group_columns].sum(axis=1)
    assert ((group_sums - tb_wide["total"]).abs() < 2.0).all(), "Chart groups do not sum to 24 hours."

    # Regression guard: reproduce the previous edition's published values for a survey that has
    # not changed since (China 2008). If China's survey updates, drop this block.
    china = tb_groups[(tb_groups["country"] == "China") & (tb_groups["sex"] == "total")]
    if china["year"].item() == 2008:
        expected = {"paid_work": 314.8, "sleep": 541.6, "tv_and_radio": 126.9, "total_leisure": 227.8}
        for column, value in expected.items():
            assert abs(china[column].item() - value) < 0.5, f"China 2008 {column} no longer matches the old chart."

    # Sub-activities should reconcile with their top-level category; a handful of countries leave
    # minutes unallocated (they stay in the remainder groups), so surface rather than fail.
    for parent, prefix in [
        ("paid_work_or_study", "1."),
        ("unpaid_work", "2."),
        ("personal_care", "3."),
        ("leisure", "4."),
        ("other_activities", "5."),
    ]:
        children = [
            column for code, column in CODE_TO_COLUMN.items() if code.startswith(prefix) and code.count(".") == 1
        ]
        gap = (tb_wide[parent] - tb_wide[children].fillna(0).sum(axis=1)).abs()
        unallocated = tb_wide.loc[gap > 1.0, ["country", "sex"]].to_records(index=False).tolist()
        if unallocated:
            log.warning(f"{parent}: minutes unallocated to sub-activities for {unallocated}")
