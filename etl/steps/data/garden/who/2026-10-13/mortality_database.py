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
# NOTE: WHO's February 2026 release disagrees on 1.5% of cells (the 2025 release: 0.2%), almost all in
# 2016-2024, where the Unintentional injuries table was refreshed but All Causes was not. Re-check next update.
PARTITION_TOLERANCE = 1e-6
PARTITION_MIN_SHARE_CONSISTENT = 0.95


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

    # Step 4: Checks on the processed data.
    _validate_country_coverage(tb)
    _validate_temporal_patterns(tb)
    _validate_cause_specific_patterns(tb)
    _validate_data_completeness(tb)
    _validate_cause_partition(tb, tb_causes)

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


def _validate_cause_partition(tb: Table, tb_causes: Table) -> None:
    """The cause groups that partition All Causes must add up to it.

    The partition is every ingested group whose parent is All Causes or a group we do not ingest; a
    group whose parent is itself ingested (a cancer type under Malignant neoplasms, say) is a drill-down
    and must not be counted twice. Checked on death counts per country, year, sex and age group, on cells
    where every partition group WHO ever reports for that sex and age band has a row (it reports Maternal
    conditions for females only, for instance). Cells of unknown age are skipped: WHO allocates those
    deaths inconsistently.

    An error in our own processing (a dropped group, a drill-down counted as a partition group) breaks
    the sum on essentially every cell, so the build fails when fewer than PARTITION_MIN_SHARE_CONSISTENT
    of the cells agree. A small set of disagreeing cells means WHO's per-cause tables are out of step with
    each other for some country-years; those are logged so the update's report can list them.
    """
    ingested = set(tb_causes["cause"])
    partition = [
        cause
        for cause in ingested
        if cause != "All Causes" and (CAUSE_TREE[cause][1] == "All Causes" or CAUSE_TREE[cause][1] not in ingested)
    ]
    keys = ["country", "year", "sex", "age_group"]

    total = tb.loc[tb["cause"] == "All Causes", keys + ["number"]].rename(columns={"number": "total"})
    parts = tb[tb["cause"].isin(partition)]
    summary = parts.groupby(keys, observed=True).agg(
        parts=("number", lambda values: values.sum(min_count=1)), n_groups=("cause", "nunique")
    )
    # The groups a cell must have: those WHO ever reports for its sex and age band.
    required = (
        parts[["sex", "age_group", "cause"]]
        .drop_duplicates()
        .groupby(["sex", "age_group"], observed=True)
        .agg(n_required=("cause", "nunique"))
    )
    cells = pr.merge(total, summary.reset_index(), on=keys)
    cells = pr.merge(cells, required.reset_index(), on=["sex", "age_group"])

    complete = cells[
        (cells["n_groups"] == cells["n_required"])
        & (cells["total"] > 0)
        & cells["parts"].notna()
        & (cells["age_group"] != "Unknown age")
    ]
    deviation = (complete["parts"].astype(float) / complete["total"].astype(float) - 1).abs()
    off = complete[(deviation > PARTITION_TOLERANCE).fillna(False)]
    share_consistent = 1 - len(off) / len(complete)

    assert share_consistent >= PARTITION_MIN_SHARE_CONSISTENT, (
        f"The {len(partition)} partition groups add up to All Causes on only {share_consistent:.1%} of "
        f"{len(complete):,} cells, which points at our processing rather than the source"
    )
    if len(off):
        country_years = Counter(zip(off["country"].astype(str), off["year"].astype(int)))
        log.warning(
            "Cause groups do not add up to All Causes on some cells: WHO's per-cause tables disagree for these country-years",
            cells=len(off),
            share=f"{len(off) / len(complete):.2%}",
            country_years=len(country_years),
            worst=[f"{country} {year}: {n} cells" for (country, year), n in country_years.most_common(8)],
        )
    log.info("cause_partition", partition_groups=len(partition), cells_checked=len(complete), cells_off=len(off))
