"""Load the WHO Mortality Database meadow dataset and create the garden dataset.

Two tables come out:

- `mortality_database`: deaths, death rates, age-standardized death rates and shares of total deaths,
  by country, year, sex, age group and cause group.
- `causes`: one row per cause group, with its depth in WHO's cause tree (`cause_level`), its parent
  group and the ICD-10 codes that define it. The codes are a property of the cause, not a dimension
  of the data, so they live here rather than in the fact table's key.

WHO's cause tree has four top-level groups: communicable/maternal/perinatal/nutritional conditions,
noncommunicable diseases, injuries, and ill-defined diseases. The groups ingested here sit one level
below them and together account for every registered death exactly once, which `_validate_cause_partition`
checks. We ingest none of the four top-level groups themselves, except "Ill-defined diseases", which
has no children, so `parent_cause` can name a group that is not itself a row.

One correction is applied to WHO's numbers: `correct_unintentional_injuries` removes the accidental
poisonings that the February 2026 release counts twice under Unintentional injuries (details there).
"""

from collections import Counter

import numpy as np
import pandas as pd
from owid.catalog import Table
from owid.catalog import processing as pr
from structlog import get_logger

from etl.helpers import PathFinder

log = get_logger()

paths = PathFinder(__file__)

# WHO's labels for its four top-level groups.
COMMUNICABLE = "Communicable, maternal, perinatal and nutritional conditions"
NONCOMMUNICABLE = "Noncommunicable diseases"
INJURIES = "Injuries"

# Our label for each cause group -> (depth in WHO's cause tree, parent group).
# Depth 0 is All Causes, depth 1 WHO's four top-level groups, depth 2 the groups beneath them.
CAUSE_TREE: dict[str, tuple[int, str | None]] = {
    "All Causes": (0, None),
    "Infectious and parasitic diseases": (2, COMMUNICABLE),
    "Respiratory infections": (2, COMMUNICABLE),
    "Maternal conditions": (2, COMMUNICABLE),
    "Perinatal conditions": (2, COMMUNICABLE),
    "Nutritional deficiencies": (2, COMMUNICABLE),
    "Malignant neoplasms": (2, NONCOMMUNICABLE),
    "Other neoplasms": (2, NONCOMMUNICABLE),
    "Diabetes mellitus, blood and endocrine disorders": (2, NONCOMMUNICABLE),
    "Neuropsychiatric conditions": (2, NONCOMMUNICABLE),
    "Sense organ diseases": (2, NONCOMMUNICABLE),
    "Cardiovascular diseases": (2, NONCOMMUNICABLE),
    "Respiratory diseases": (2, NONCOMMUNICABLE),
    "Digestive diseases": (2, NONCOMMUNICABLE),
    "Genitourinary diseases": (2, NONCOMMUNICABLE),
    "Skin diseases": (2, NONCOMMUNICABLE),
    "Musculoskeletal diseases": (2, NONCOMMUNICABLE),
    "Congenital anomalies": (2, NONCOMMUNICABLE),
    "Oral conditions": (2, NONCOMMUNICABLE),
    "Sudden infant death syndrome": (2, NONCOMMUNICABLE),
    "Unintentional injuries": (2, INJURIES),
    "Intentional injuries": (2, INJURIES),
    "Ill-defined injuries": (2, INJURIES),
    "Ill-defined diseases": (1, "All Causes"),
}

# WHO labels we change for charts. The first says what the group contains; the second is shorter.
CAUSE_RENAMES = {
    "Diabetes mellitus and endocrine disorders": "Diabetes mellitus, blood and endocrine disorders",
    "Ill-defined injuries/accidents": "Ill-defined injuries",
}

SEX_RENAMES = {"All": "Both sexes", "Female": "Females", "Male": "Males", "Unknown": "Unknown sex"}

AGE_GROUP_RENAMES = {
    "[Unknown]": "Unknown age",
    "[85+]": "over 85 years",
    "[80-84]": "80-84 years",
    "[75-79]": "75-79 years",
    "[70-74]": "70-74 years",
    "[65-69]": "65-69 years",
    "[60-64]": "60-64 years",
    "[55-59]": "55-59 years",
    "[50-54]": "50-54 years",
    "[45-49]": "45-49 years",
    "[40-44]": "40-44 years",
    "[35-39]": "35-39 years",
    "[30-34]": "30-34 years",
    "[25-29]": "25-29 years",
    "[20-24]": "20-24 years",
    "[15-19]": "15-19 years",
    "[10-14]": "10-14 years",
    "[5-9]": "5-9 years",
    "[1-4]": "1-4 years",
    "[0]": "less than 1 year",
    "[All]": "all ages",
}

# The cause groups directly under All Causes must add up to All Causes, to this relative tolerance per cell.
# The build fails only when fewer than PARTITION_MIN_SHARE_CONSISTENT of the cells agree: an error in our own
# processing breaks essentially every cell, whereas WHO's per-cause tables can be out of step with each other
# for some country-years, which is theirs to fix and ours to report (see _validate_cause_partition).
PARTITION_TOLERANCE = 1e-6
PARTITION_MIN_SHARE_CONSISTENT = 0.95
# After correct_unintentional_injuries has run, only WHO's small pre-existing residuals should remain.
PARTITION_MIN_SHARE_AFTER_CORRECTION = 0.99

# NOTE: WHO's February 2026 release disagrees on 1.5% of cells (the 2025 release: 0.2%), almost all in
# 2016-2024. All Causes matches national statistics offices to the unit; the group that is off is
# Unintentional injuries, which counts the accidental poisonings X41, X42, X44 and X45 that its own
# definition excludes and that Neuropsychiatric conditions already contains. correct_unintentional_injuries
# removes them where the country-year's headline cell overshoots by more than MATERIAL_EXCESS. The threshold
# separates the two kinds of gap the data holds: WHO's pre-existing residuals (Japan 2022-23, Brazil 2021-22,
# Thailand 2023, Turkiye 2013-17; present in the 2025 release too, and not explained by the Unintentional
# injuries change) all sit at or below 0.02% of all deaths, while every double count that could be checked
# against the 2025 release sits at or above 0.05%. In a small country 0.05% is a single death, so a floor of
# MATERIAL_EXCESS_DEATHS keeps one-death discrepancies (Belize 2007-08, Maldives 2010: 1-3 deaths) out of the
# correction; the smallest double count is Luxembourg 2024 with 14. Re-check next update: if WHO has fixed its
# table, the correction finds nothing to do and can be deleted.
CORRECTED_CAUSE = "Unintentional injuries"
MATERIAL_EXCESS = 0.0005
MATERIAL_EXCESS_DEATHS = 10


def run() -> None:
    # Step 1: Load the meadow table and check it is what we expect.
    ds_meadow = paths.load_dataset("mortality_database")
    tb = ds_meadow.read("mortality_database", safe_types=False)
    _validate_input_data(tb)
    _validate_mortality_values(tb)
    _validate_demographic_dimensions(tb)

    # Step 2: Readable labels, our cause names, harmonized country names.
    tb = _rename_categories(tb, "sex", SEX_RENAMES)
    tb = _rename_categories(tb, "age_group", AGE_GROUP_RENAMES)
    tb = _rename_categories(tb, "cause", CAUSE_RENAMES)
    tb = paths.regions.harmonize_names(tb)

    # Step 3: Split the cause definitions into their own table. The ICD-10 codes identify the cause and
    # nothing else, so the fact table loses nothing by dropping them.
    tb_causes = build_causes_table(tb)
    tb = tb.drop(columns=["icd10_codes"])

    # Step 4: Checks on the processed data, then the one correction to WHO's numbers, then the check again.
    _validate_country_coverage(tb)
    _validate_temporal_patterns(tb)
    _validate_cause_specific_patterns(tb)
    _validate_data_completeness(tb)
    _validate_cause_partition(tb, tb_causes, min_share_consistent=PARTITION_MIN_SHARE_CONSISTENT, stage="as published")
    tb = correct_unintentional_injuries(tb, tb_causes)
    _validate_cause_partition(
        tb, tb_causes, min_share_consistent=PARTITION_MIN_SHARE_AFTER_CORRECTION, stage="after correction"
    )

    tb = tb.format(["country", "year", "sex", "age_group", "cause"], short_name="mortality_database")
    tb_causes = tb_causes.format(["cause"], short_name="causes")

    # Step 5: Save.
    ds_garden = paths.create_dataset(
        tables=[tb, tb_causes], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )
    ds_garden.save()


def _rename_categories(tb: Table, column: str, renames: dict[str, str]) -> Table:
    """Rename the values of a categorical column, keeping the column's metadata."""
    metadata = tb[column].metadata
    if isinstance(tb[column].dtype, pd.CategoricalDtype):
        tb[column] = tb[column].cat.rename_categories(lambda value: renames.get(value, value))
    else:
        tb[column] = tb[column].replace(renames)
    tb[column].metadata = metadata  # ty: ignore
    return tb


def build_causes_table(tb: Table) -> Table:
    """One row per cause group: its ICD-10 codes, from the data, and its place in WHO's tree, from CAUSE_TREE."""
    tb_causes = tb[["cause", "icd10_codes"]].drop_duplicates().reset_index(drop=True)

    # WHO's code strings space their commas unevenly ("D50-D53,D64.9", "J00-J22,  P23"); normalize so they
    # read cleanly and match the strings repeated in the metadata.
    codes_metadata = tb_causes["icd10_codes"].metadata
    tb_causes["icd10_codes"] = (
        tb_causes["icd10_codes"]
        .astype(str)
        .str.replace(r"\s*,\s*", ", ", regex=True)
        .str.replace(r"\s{2,}", " ", regex=True)
        .str.strip()
    )
    tb_causes["icd10_codes"].metadata = codes_metadata  # ty: ignore

    # Dropping the codes from the fact table is only safe while each cause has exactly one code string.
    assert tb_causes["cause"].is_unique, "A cause group carries more than one ICD-10 code string"
    assert tb_causes["icd10_codes"].is_unique, "An ICD-10 code string is shared by two cause groups"

    in_data, in_tree = set(tb_causes["cause"]), set(CAUSE_TREE)
    assert in_data == in_tree, (
        f"Cause groups in the data but not in CAUSE_TREE: {sorted(in_data - in_tree)}; "
        f"in CAUSE_TREE but not in the data: {sorted(in_tree - in_data)}"
    )

    tb_causes["cause_level"] = [CAUSE_TREE[cause][0] for cause in tb_causes["cause"]]
    tb_causes["parent_cause"] = [CAUSE_TREE[cause][1] for cause in tb_causes["cause"]]
    # The tree is WHO's; the codes column already carries WHO's origin, so the new columns borrow it.
    for column in ["cause_level", "parent_cause"]:
        tb_causes[column] = tb_causes[column].copy_metadata(tb_causes["icd10_codes"])
    return tb_causes


def _validate_input_data(tb: Table) -> None:
    """Basic integrity checks on the meadow table."""
    assert len(tb) > 0, "Dataset is empty"

    required_cols = ["country", "year", "sex", "age_group", "cause", "icd10_codes", "number"]
    missing_cols = [col for col in required_cols if col not in tb.columns]
    assert not missing_cols, f"Missing required columns: {missing_cols}"

    key_cols = ["country", "year", "sex", "age_group", "cause"]
    duplicates = tb.duplicated(subset=key_cols)
    assert not duplicates.any(), f"Found {duplicates.sum()} duplicate records"

    assert pd.api.types.is_numeric_dtype(tb["number"]), "Deaths count should be numeric"
    assert pd.api.types.is_integer_dtype(tb["year"]), "Year should be integer type"


def _validate_mortality_values(tb: Table) -> None:
    """Value ranges: no negative counts; flag implausibly high rates for review."""
    negative_deaths = tb[tb["number"] < 0]
    assert len(negative_deaths) == 0, f"Found {len(negative_deaths)} negative death counts"

    rate_col = "age_standardized_death_rate_per_100_000_standard_population"
    rates = tb[rate_col].dropna()
    extreme_rates = rates[(rates < 0) | (rates > 10000)]
    if len(extreme_rates) > 0:
        log.warning(
            "Extreme age-standardized rates found",
            count=len(extreme_rates),
            max_rate=extreme_rates.max(),
            min_rate=extreme_rates.min(),
        )

    death_rates = tb["death_rate_per_100_000_population"].dropna()
    extreme_rates = death_rates[death_rates > 50000]
    if len(extreme_rates) > 0:
        log.warning(
            "Extremely high death rates found",
            count=len(extreme_rates),
            examples=tb[tb["death_rate_per_100_000_population"].isin(extreme_rates)][
                ["country", "year", "sex", "age_group", "cause", "death_rate_per_100_000_population"]
            ]
            .head()
            .to_dict("records"),
        )


def _validate_demographic_dimensions(tb: Table) -> None:
    """WHO's sex and age-group codes, before renaming."""
    unexpected_sex = set(tb["sex"].unique()) - set(SEX_RENAMES)
    assert not unexpected_sex, f"Unexpected sex categories: {unexpected_sex}"

    unexpected_ages = set(tb["age_group"].unique()) - set(AGE_GROUP_RENAMES)
    if unexpected_ages:
        log.warning("Unexpected age group categories", categories=list(unexpected_ages))


def _validate_temporal_patterns(tb: Table) -> None:
    """Year range, and large year-on-year jumps in all-cause deaths for a few large countries."""
    min_year, max_year = 1950, 2025
    assert tb["year"].min() >= min_year, f"Year too early: {tb['year'].min()}"
    assert tb["year"].max() <= max_year, f"Year too late: {tb['year'].max()}"

    for country in ["United States", "Germany", "Japan", "Brazil"]:
        country_data = tb[
            (tb["country"] == country)
            & (tb["cause"] == "All Causes")
            & (tb["sex"] == "Both sexes")
            & (tb["age_group"] == "all ages")
        ].sort_values("year")

        if len(country_data) < 3:
            continue

        # Counts are unsigned integers; cast before differencing, or a drop wraps around to a huge jump.
        deaths = country_data["number"].astype(float).values
        year_changes = np.diff(deaths) / deaths[:-1] * 100

        extreme_changes = np.abs(year_changes) > 50
        if extreme_changes.any():
            problem_years = country_data["year"].iloc[1:][extreme_changes]
            log.warning(
                "Extreme year-over-year changes in deaths",
                country=country,
                years=problem_years.tolist(),
                max_change=f"{year_changes[extreme_changes].max():.1f}%",
            )


def _validate_country_coverage(tb: Table) -> None:
    """Enough countries, and the large reporters among them. Runs after harmonization, on our names."""
    countries = set(tb["country"].unique())

    min_countries = 100
    assert len(countries) >= min_countries, f"Only {len(countries)} countries found"

    # China and India do not report to the database; the rest should be present.
    major_countries = {"United States", "Germany", "United Kingdom", "France", "Japan", "Brazil", "Russia"}
    missing_major = major_countries - countries
    if missing_major:
        log.warning("Missing major countries/entities", missing=list(missing_major))


def _validate_cause_specific_patterns(tb: Table) -> None:
    """Causes that can only occur in part of the population."""
    maternal_data = tb[tb["cause"] == "Maternal conditions"]
    if len(maternal_data) > 0:
        maternal_males = maternal_data[maternal_data["sex"] == "Males"]
        assert len(maternal_males) == 0, f"Found {len(maternal_males)} male maternal deaths"

        maternal_elderly = maternal_data[maternal_data["age_group"].isin(["over 85 years", "80-84 years"])]
        if len(maternal_elderly) > 0:
            elderly_deaths = maternal_elderly["number"].sum()
            total_maternal = maternal_data["number"].sum()
            if elderly_deaths / total_maternal > 0.01:
                log.warning(
                    "High proportion of elderly maternal deaths",
                    elderly_pct=f"{elderly_deaths / total_maternal * 100:.2f}%",
                )

    sids_data = tb[tb["cause"] == "Sudden infant death syndrome"]
    if len(sids_data) > 0:
        allowed_age_groups = ["less than 1 year", "all ages", "Unknown age"]
        sids_adults = sids_data[
            (~sids_data["age_group"].isin(allowed_age_groups))
            & (sids_data["number"] > 0)
            & (sids_data["number"].notna())
        ]
        if len(sids_adults) > 0:
            log.warning(
                "SIDS deaths in non-infant age groups",
                count=len(sids_adults),
                age_groups=sids_adults["age_group"].unique().tolist(),
                countries=sids_adults["country"].unique().tolist(),
            )


def _validate_data_completeness(tb: Table) -> None:
    """Missing counts, and gaps in recent years for a few large reporters."""
    missing_share = tb["number"].isna().mean()
    if missing_share > 0.05:
        log.warning("High proportion of missing death counts", missing_pct=f"{missing_share * 100:.2f}%")

    for country in ["United States", "Germany", "Japan"]:
        country_data = tb[tb["country"] == country]
        if len(country_data) == 0:
            continue

        years_available = set(country_data["year"].unique())
        expected_years = set(range(2000, 2024))
        missing_years = expected_years - years_available

        if len(missing_years) > 5:
            log.warning(
                "Significant temporal gaps",
                country=country,
                missing_years=len(missing_years),
                sample_missing=sorted(list(missing_years))[:5],
            )


def _partition_groups(tb_causes: Table) -> list[str]:
    """The ingested groups that partition All Causes: those whose parent is All Causes or a group we do not ingest.

    A group whose parent is itself ingested (a cancer type under Malignant neoplasms, say) is a drill-down
    and must not be counted twice.
    """
    ingested = set(tb_causes["cause"])
    return [
        cause
        for cause in ingested
        if cause != "All Causes" and (CAUSE_TREE[cause][1] == "All Causes" or CAUSE_TREE[cause][1] not in ingested)
    ]


def _partition_cells(tb: Table, tb_causes: Table) -> Table:
    """Per country, year, sex and age band: All Causes, the sum of the partition groups, and the gap between them.

    Keeps only cells where every partition group WHO ever reports for that sex and age band has a row (it
    reports Maternal conditions for females only, for instance), with a positive All Causes count and a
    known age band, because WHO allocates deaths of unknown age inconsistently across groups.
    """
    keys = ["country", "year", "sex", "age_group"]
    total = tb.loc[tb["cause"] == "All Causes", keys + ["number", "death_rate_per_100_000_population"]].rename(
        columns={"number": "total", "death_rate_per_100_000_population": "total_rate"}
    )
    parts = tb[tb["cause"].isin(_partition_groups(tb_causes))]
    summary = parts.groupby(keys, observed=True).agg(
        parts=("number", lambda values: values.sum(min_count=1)), n_groups=("cause", "nunique")
    )
    required = (
        parts[["sex", "age_group", "cause"]]
        .drop_duplicates()
        .groupby(["sex", "age_group"], observed=True)
        .agg(n_required=("cause", "nunique"))
    )
    cells = pr.merge(total, summary.reset_index(), on=keys)
    cells = pr.merge(cells, required.reset_index(), on=["sex", "age_group"])
    cells = cells[
        (cells["n_groups"] == cells["n_required"])
        & (cells["total"] > 0)
        & cells["parts"].notna()
        & (cells["age_group"] != "Unknown age")
    ].copy()
    cells["excess"] = cells["parts"].astype(float) - cells["total"].astype(float)
    cells["deviation"] = (cells["excess"] / cells["total"].astype(float)).abs()
    return cells


def _validate_cause_partition(tb: Table, tb_causes: Table, min_share_consistent: float, stage: str) -> None:
    """The cause groups that partition All Causes must add up to it on (almost) every cell.

    An error in our own processing (a dropped group, a drill-down counted as a partition group) breaks
    the sum on essentially every cell, so the build fails when fewer than `min_share_consistent` of the
    cells agree. A small set of disagreeing cells means WHO's per-cause tables are out of step with each
    other for some country-years; those are logged so the update's report can list them.
    """
    cells = _partition_cells(tb, tb_causes)
    off = cells[(cells["deviation"] > PARTITION_TOLERANCE).fillna(False)]
    share_consistent = 1 - len(off) / len(cells)

    assert share_consistent >= min_share_consistent, (
        f"{stage}: the partition groups add up to All Causes on only {share_consistent:.1%} of {len(cells):,} cells, "
        "which points at our processing rather than the source"
    )
    if len(off):
        country_years = Counter(zip(off["country"].astype(str), off["year"].astype(int)))
        log.warning(
            "Cause groups do not add up to All Causes on some cells",
            stage=stage,
            cells=len(off),
            share=f"{len(off) / len(cells):.2%}",
            country_years=len(country_years),
            worst=[f"{country} {year}: {n} cells" for (country, year), n in country_years.most_common(8)],
        )
    log.info("cause_partition", stage=stage, cells_checked=len(cells), cells_off=len(off))


def correct_unintentional_injuries(tb: Table, tb_causes: Table) -> Table:
    """Remove the accidental poisonings that WHO's February 2026 release counts twice under Unintentional injuries.

    In that release, for some country-years, Unintentional injuries includes the accidental drug and alcohol
    poisonings X41, X42, X44 and X45 that its own definition excludes and that Neuropsychiatric conditions
    already holds, so the partition groups add up to more than All Causes. All Causes itself matches the
    national statistics offices to the unit (checked for Canada, Australia and the United Kingdom), and the
    other groups were revised consistently with it, so the gap is the double count.

    A country-year is affected when its both-sexes, all-ages cell overshoots All Causes by more than
    MATERIAL_EXCESS and by at least MATERIAL_EXCESS_DEATHS deaths; WHO's small pre-existing residuals (Japan,
    Brazil, Thailand, Turkiye) and one-death discrepancies in small countries stay below and are left alone. In an affected country-year, every known-age cell that overshoots by at least one death gets
    Unintentional injuries = All Causes minus the other groups, its share of deaths recomputed from that, and
    its crude death rate from WHO's own population, recovered as All Causes deaths / All Causes death rate.
    The age-standardized rate cannot be rebuilt from the published numbers and is left missing.
    """
    keys = ["country", "year", "sex", "age_group"]
    cells = _partition_cells(tb, tb_causes)
    headline = cells[(cells["sex"] == "Both sexes") & (cells["age_group"] == "all ages")]
    affected = headline.loc[
        (headline["excess"] >= MATERIAL_EXCESS_DEATHS) & (headline["deviation"] > MATERIAL_EXCESS), ["country", "year"]
    ]
    if affected.empty:
        log.info("unintentional_injuries_correction", country_years=0, cells=0)
        return tb

    fix = pr.merge(cells, affected, on=["country", "year"])
    fix = fix.loc[fix["excess"] >= 1, keys + ["total", "total_rate", "excess"]]

    tb = pr.merge(tb, fix, on=keys, how="left")
    target = (tb["cause"] == CORRECTED_CAUSE) & tb["excess"].notna()
    corrected = tb.loc[target, "number"].astype(float) - tb.loc[target, "excess"]
    # A cell whose Unintentional injuries count is smaller than the gap does not fit the mechanism: leave it.
    misfit = corrected < 0
    if misfit.any():
        log.warning("unintentional_injuries_correction: cells left as published", cells=int(misfit.sum()))
        target[target[target].index[misfit]] = False
        corrected = corrected[~misfit]

    total = tb.loc[target, "total"].astype(float)
    population = (total / tb.loc[target, "total_rate"].astype(float) * 100_000).where(tb.loc[target, "total_rate"] > 0)
    tb.loc[target, "number"] = corrected.round().astype(tb["number"].dtype)
    tb.loc[target, "percentage_of_cause_specific_deaths_out_of_total_deaths"] = corrected / total * 100
    tb.loc[target, "death_rate_per_100_000_population"] = corrected / population * 100_000
    tb.loc[target, "age_standardized_death_rate_per_100_000_standard_population"] = np.nan

    # Rows of the group in affected country-years that the check could not see (incomplete or unknown-age cells).
    in_affected = pr.merge(tb.loc[tb["cause"] == CORRECTED_CAUSE, keys], affected, on=["country", "year"])
    log.warning(
        "unintentional_injuries_correction",
        country_years=len(affected),
        cells_corrected=int(target.sum()),
        deaths_removed=int(tb.loc[target, "excess"].sum()),
        rows_in_affected_country_years_left_as_published=len(in_affected) - int(target.sum()),
        example=_describe_correction(tb, target, "Canada", 2021),
    )
    return tb.drop(columns=["total", "total_rate", "excess"])


def _describe_correction(tb: Table, target: pd.Series, country: str, year: int) -> str:
    """One corrected headline cell, for the log."""
    row = tb[
        target
        & (tb["country"] == country)
        & (tb["year"] == year)
        & (tb["sex"] == "Both sexes")
        & (tb["age_group"] == "all ages")
    ]
    if row.empty:
        return f"{country} {year}: not corrected"
    r = row.iloc[0]
    return f"{country} {year}: {r['number'] + r['excess']:,.0f} -> {r['number']:,.0f} deaths (All Causes {r['total']:,.0f})"
