"""Harmonize the OECD Time Use Database.

The source reports average minutes per day in a hierarchy of activities: five top-level categories
(paid work or study, unpaid work, personal care, leisure, other) with detailed sub-activities. This
step publishes that hierarchy as the source gives it, one column per activity code, harmonizing the
country names and dating each country by the year its survey's fieldwork ended.

**No display groups.** A `time_use_chart_groups` table used to sit beside this one, carrying ten
groups built for the "How do people spend their time?" chart. It went because the chart draws the
source's four top-level categories and the ten were only ever summed back up to them — a
reconstruction accurate to 3e-05 minutes of columns this table already has, and none of its 33
indicators was used by any chart. What the chart does on top of the source is a single addition
(unpaid work plus the small "other" category), which is a presentation choice and lives in
`export://static_viz/oecd/2026-08-14/time_use_by_country`.

Some countries leave part of a top-level category unallocated to any sub-activity — Poland 37
minutes a day of unpaid work and Japan 22, which is 15-18% of the category — so a top-level total is
NOT the sum of its published sub-activities. `sanity_check_outputs` surfaces every such gap rather
than failing, because it is the source's own reporting and not something this step can fix.
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

# The five top-level categories and the source's own total. These must be present for every country
# and sex: they are what the day-closes check reconciles and what the static viz draws, so a missing
# one is not a gap in the data but a broken read.
#
# The sub-activity codes are deliberately NOT here. They used to be, because the ten display groups
# were built from seven of them; nothing is built from them now, and whether a country breaks an
# activity out is the source's business. `sanity_check_outputs` asserts instead that no published
# column is entirely empty, which is the failure this list was really guarding against.
REQUIRED_CODES = ["1", "2", "3", "4", "5", "T"]

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
    tb = paths.regions.harmonize_names(tb)

    # The dataset has no time dimension of its own: each country reports one survey, whose year(s)
    # the source states as a label like "2009/10". Use the survey's end year as the year.
    tb["year"] = tb["survey_year"].map(parse_survey_end_year).copy_metadata(tb["minutes"])

    tb_wide = pivot_activities(tb)

    sanity_check_outputs(tb_wide)

    # The source's own total row (~1440 everywhere by construction) is only needed for the checks.
    tb_wide = tb_wide.drop(columns=["total"])

    # Applied here, after the checks: they reconcile what the OECD publishes, and one of them
    # reproduces the previous edition's published values for China 2008, which the cutoff removes.
    tb_wide = apply_survey_year_cutoff(tb_wide)

    tb_wide = tb_wide.format(["country", "year", "sex"], short_name="time_use")

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb_wide], default_metadata=ds_meadow.metadata)
    ds_garden.save()


def apply_survey_year_cutoff(tb_wide: Table) -> Table:
    """Keep only the countries whose survey is from `EARLIEST_SURVEY_YEAR` on."""
    if EARLIEST_SURVEY_YEAR is None:
        return tb_wide

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
    sanity_check_cutoff(tb_wide)
    return tb_wide


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


def sanity_check_cutoff(tb_wide: Table) -> None:
    """Check what the cutoff left: the expected countries, and one reference age."""
    countries = set(tb_wide["country"])
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


# The source's five top-level categories, which are what everything downstream reads.
TOP_LEVEL_COLUMNS = ["paid_work_or_study", "unpaid_work", "personal_care", "leisure", "other_activities"]


def sanity_check_outputs(tb_wide: Table) -> None:
    assert tb_wide[TOP_LEVEL_COLUMNS].notna().all().all(), "A top-level category has missing values."
    # Every activity is published as an indicator, so an all-empty column is a broken read rather than
    # a country choosing not to report — no code in the source is unreported by all 35 countries.
    empty = [column for column in CODE_TO_COLUMN.values() if column in tb_wide and tb_wide[column].isna().all()]
    assert not empty, f"These activity columns are entirely empty: {empty}."
    assert (tb_wide[TOP_LEVEL_COLUMNS] >= 0).all().all(), "A top-level category is negative."

    # The five partition the day: they must add up to the source's own normalized total.
    # NOTE: the source's own categories deviate from its total row by up to ~1.2 minutes for Japan and
    # New Zealand (women) — rounding on the source's side — hence the 2-minute bar.
    sums = tb_wide[TOP_LEVEL_COLUMNS].sum(axis=1)
    assert ((sums - tb_wide["total"]).abs() < 2.0).all(), "The top-level categories do not sum to 24 hours."

    # Regression guard: reproduce the previous edition's published values for a survey that has not
    # changed since (China 2008). If China's survey updates, drop this block.
    #
    # These four were the previous chart's display groups, and three of them are source columns
    # outright. `paid_work` was the derived one — the paid-work-or-study category less the two study
    # activities — so it is written out here rather than read; all four still reproduce to 0.05 min.
    china = tb_wide[(tb_wide["country"] == "China") & (tb_wide["sex"] == "total")]
    if china["year"].item() == 2008:
        study = china["time_in_school_or_classes"].item() + (china["research_and_homework"].fillna(0)).item()
        expected = {
            "paid work, less study": (china["paid_work_or_study"].item() - study, 314.8),
            "sleeping": (china["sleeping"].item(), 541.6),
            "tv or radio at home": (china["tv_or_radio_at_home"].item(), 126.9),
            "leisure": (china["leisure"].item(), 227.8),
        }
        for label, (got, want) in expected.items():
            assert abs(got - want) < 0.5, f"China 2008 {label} is {got:.1f}, was {want} in the previous edition."

    # A top-level total is NOT always the sum of its published sub-activities: some countries do not
    # break every activity out. Surface each gap with its size rather than failing — it is the
    # source's own reporting, and the amounts are what say whether it matters. Poland leaves 37 min/day
    # of unpaid work unassigned and Japan 22, which is 15-18% of the category; everything else is under
    # a minute of rounding. Downstream, those minutes stay inside their own top-level category, so no
    # category total is affected.
    for parent, prefix in [(column, f"{code}.") for code, column in CODE_TO_COLUMN.items() if code in "12345"]:
        children = [
            column for code, column in CODE_TO_COLUMN.items() if code.startswith(prefix) and code.count(".") == 1
        ]
        gap = tb_wide[parent] - tb_wide[children].fillna(0).sum(axis=1)
        flagged = tb_wide.loc[gap.abs() > 1.0]
        if len(flagged):
            detail = ", ".join(
                f"{row['country']} ({row['sex']}) {gap[index]:+.0f} min, {100 * gap[index] / row[parent]:+.0f}%"
                for index, row in flagged.iterrows()
            )
            log.warning(f"{parent}: minutes not assigned to any sub-activity — {detail}")
