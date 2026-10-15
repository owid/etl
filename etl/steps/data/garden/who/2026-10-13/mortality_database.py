"""Load the WHO Mortality Database meadow dataset and create the garden dataset.

Three tables come out:

- `mortality_database`: deaths, death rates, age-standardized death rates and shares of total deaths,
  by country, year, sex and age group, for All Causes and the 23 cause groups that tile it.
- `vaccine_preventable_diseases`: the same four indicators for the seven vaccine-preventable causes,
  which are drill-downs inside one of those groups.
- `intentional_injuries`: the same four indicators for self-inflicted injuries and violence (homicides), two
  drill-downs inside Intentional injuries; violence also gets the age bands the homicide charts use.
- `cancers`: the same four indicators for the 25 cancer types inside Malignant neoplasms, plus two age
  bands WHO does not publish (`<5 years`, `<10 years`), derived in `add_derived_age_groups` for the types
  and for Malignant neoplasms itself (in `mortality_database`), which the childhood-cancer charts plot together.
- `most_common_cancer`: the cancer type with the highest age-standardized death rate, per country, year
  and sex.
- `causes`: one row per cause group, with its depth in WHO's cause tree (`cause_level`), its parent
  group, the ICD-10 codes that define it and the table it is published in. The codes are a property of
  the cause, not a dimension of the data, so they live here rather than in the fact tables' key.

WHO's cause tree has four top-level groups: communicable/maternal/perinatal/nutritional conditions,
noncommunicable diseases, injuries, and ill-defined diseases. The 23 groups one level below them together
account for every registered death exactly once, which `_validate_cause_partition` checks. Below that tier
we also ingest drill-downs (the vaccine-preventable diseases inside Infectious and parasitic diseases);
`causes` records where each sits, and the partition check leaves them out. We ingest none of the four
top-level groups themselves, except "Ill-defined diseases", which has no children, so `parent_cause` can
name a group that is not itself a row.

One correction is applied to WHO's numbers: `correct_unintentional_injuries` removes the accidental
poisonings that the February 2026 release counts twice under Unintentional injuries (details there).
"""

from collections import Counter
from datetime import date
from pathlib import Path

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
INFECTIOUS = "Infectious and parasitic diseases"
CHILDHOOD_CLUSTER = "Childhood-cluster diseases"
MALIGNANT = "Malignant neoplasms"
VIOLENCE = "Violence"

# WHO's 23 groups that tile All Causes. Fold-ins add drill-downs inside them, never new partition groups.
N_PARTITION_GROUPS = 23

# The table each drill-down cause is published in; every other cause goes in MAIN_TABLE, the partition tier.
# One table per group of drill-downs keeps the layer visible in the catalog path, which the admin's flat list
# of indicators does not show.
MAIN_TABLE = "mortality_database"
# WHO's 25 cancer types, the children of Malignant neoplasms (formerly the mortality_database_cancer dataset).
CANCERS = [
    "Mouth and oropharynx cancers",
    "Oesophagus cancer",
    "Stomach cancer",
    "Colon and rectum cancers",
    "Liver cancer",
    "Pancreas cancer",
    "Trachea, bronchus, lung cancers",
    "Melanoma and other skin cancers",
    "Breast cancer",
    "Cervix uteri cancer",
    "Corpus uteri cancer",
    "Ovary cancer",
    "Prostate cancer",
    "Testicular cancer",
    "Kidney cancer",
    "Bladder cancer",
    "Brain and nervous system cancers",
    "Gallbladder and biliary tract cancer",
    "Larynx cancer",
    "Thyroid cancer",
    "Mesothelioma",
    "Lymphomas, multiple myeloma",
    "Leukaemia",
    "Ill-defined malignant neoplasms",
    "Other malignant neoplasms",
]
DRILL_DOWN_TABLES = {
    "Pertussis": "vaccine_preventable_diseases",
    "Poliomyelitis": "vaccine_preventable_diseases",
    "Diphtheria": "vaccine_preventable_diseases",
    "Measles": "vaccine_preventable_diseases",
    "Tetanus": "vaccine_preventable_diseases",
    "Hepatitis B": "vaccine_preventable_diseases",
    "Japanese encephalitis": "vaccine_preventable_diseases",
    "Self-inflicted injuries": "intentional_injuries",
    VIOLENCE: "intentional_injuries",
    **{cancer: "cancers" for cancer in CANCERS},
}

# Age bands WHO does not publish, built per cause group by summing WHO's bands, for the causes whose charts use
# them. The rate divides by the population WHO itself used, recovered from All Causes (see add_derived_age_groups).
UNDER_5 = ["less than 1 year", "1-4 years"]
DERIVED_AGE_GROUPS: list[tuple[list[str], dict[str, list[str]]]] = [
    # The childhood-cancer charts plot the cancer types and their parent together.
    ([MALIGNANT, *CANCERS], {"<5 years": UNDER_5, "<10 years": [*UNDER_5, "5-9 years"]}),
    # The homicide charts compare victims' ages in the bands IHME uses, plus children under 20.
    (
        [VIOLENCE],
        {
            "<5 years": UNDER_5,
            "5-14 years": ["5-9 years", "10-14 years"],
            "15-49 years": [
                "15-19 years",
                "20-24 years",
                "25-29 years",
                "30-34 years",
                "35-39 years",
                "40-44 years",
                "45-49 years",
            ],
            "50-69 years": ["50-54 years", "55-59 years", "60-64 years", "65-69 years"],
            "over 70 years": ["70-74 years", "75-79 years", "80-84 years", "over 85 years"],
            "<20 years": [*UNDER_5, "5-9 years", "10-14 years", "15-19 years"],
        },
    ),
]

# Causes that occur in one sex only. Deaths recorded for the other sex are coding errors in the source;
# they are tolerated up to this share of the cause's deaths and logged.
SEX_SPECIFIC_CAUSES = {
    "Prostate cancer": "Males",
    "Testicular cancer": "Males",
    "Cervix uteri cancer": "Females",
    "Corpus uteri cancer": "Females",
    "Ovary cancer": "Females",
}
MAX_SHARE_IN_OTHER_SEX = 0.001

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
    # Drill-downs inside Infectious and parasitic diseases (formerly the vaccine_preventable dataset).
    "Pertussis": (4, CHILDHOOD_CLUSTER),
    "Poliomyelitis": (4, CHILDHOOD_CLUSTER),
    "Diphtheria": (4, CHILDHOOD_CLUSTER),
    "Measles": (4, CHILDHOOD_CLUSTER),
    "Tetanus": (4, CHILDHOOD_CLUSTER),
    "Hepatitis B": (3, INFECTIOUS),
    "Japanese encephalitis": (3, INFECTIOUS),
    # Drill-downs inside Intentional injuries (formerly the self_inflicted_injuries and homicide/who_mort_db datasets).
    "Self-inflicted injuries": (3, "Intentional injuries"),
    VIOLENCE: (3, "Intentional injuries"),
    # Drill-downs inside Malignant neoplasms (formerly the mortality_database_cancer dataset).
    **{cancer: (3, MALIGNANT) for cancer in CANCERS},
}

# Groups that appear only as parents: WHO's top-level groups, and the nodes between an ingested drill-down
# and its ingested ancestor. _ancestors walks through them.
UNINGESTED_GROUPS: dict[str, tuple[int, str | None]] = {
    COMMUNICABLE: (1, "All Causes"),
    NONCOMMUNICABLE: (1, "All Causes"),
    INJURIES: (1, "All Causes"),
    CHILDHOOD_CLUSTER: (3, INFECTIOUS),
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

    # Step 5: Age bands WHO does not publish, for the tables whose charts use them, on WHO's own population.
    tb = add_derived_age_groups(tb, tb_causes)

    # Step 6: One table per layer - the partition tier, then one table per group of drill-downs - so the catalog
    # path says where a cause sits. The metadata must know every cause too (see the check). Plus the leading
    # cancer type, whose cause is a value rather than a key, so it cannot be a row of the fact tables.
    _check_metadata_lists_every_cause(tb_causes)
    tables = _split_by_table(tb, tb_causes)
    tb_most_common_cancer = build_most_common_cancer(tb, tb_causes)
    tb_causes = tb_causes.format(["cause"], short_name="causes")

    # Step 7: Save.
    ds_garden = paths.create_dataset(
        tables=[*tables, tb_most_common_cancer, tb_causes],
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )
    ds_garden.save()


def _split_by_table(tb: Table, tb_causes: Table) -> list[Table]:
    """One formatted fact table per value of `causes.table`, the partition tier first."""
    keys = ["country", "year", "sex", "age_group", "cause"]
    tables = []
    for short_name in dict.fromkeys([MAIN_TABLE, *DRILL_DOWN_TABLES.values()]):
        causes = set(tb_causes.loc[tb_causes["table"] == short_name, "cause"])
        part = tb[tb["cause"].isin(causes)].copy()
        # A filtered categorical keeps every category; the grapher step would render empty indicators for them.
        if isinstance(part["cause"].dtype, pd.CategoricalDtype):
            metadata = part["cause"].metadata
            part["cause"] = part["cause"].cat.remove_unused_categories()
            part["cause"].metadata = metadata  # ty: ignore
        tables.append(part.format(keys, short_name=short_name))
    assert sum(len(t) for t in tables) == len(tb), "Rows lost in the split by table"
    return tables


def add_derived_age_groups(tb: Table, tb_causes: Table) -> Table:
    """Add the age bands in DERIVED_AGE_GROUPS to the causes each entry names.

    Deaths are summed over the bands each derived group spans. The death rate divides them by the population
    WHO itself divided by, which `_implied_population` recovers from All Causes. Every band must contribute
    both a death count and a population, otherwise the group is left missing: a partial numerator over a full
    denominator understates the rate, a full numerator over a partial denominator overstates it. The share of
    total deaths and the age-standardized rate are not derived.
    """
    tb_population = _implied_population(tb)
    keys = ["country", "year", "sex", "cause"]

    parts = [tb]
    for causes, groups in DERIVED_AGE_GROUPS:
        unknown = set(causes) - set(tb_causes["cause"])
        assert not unknown, f"Unknown causes in DERIVED_AGE_GROUPS: {unknown}"
        for label, bands in groups.items():
            tb_bands = tb[tb["cause"].isin(causes) & tb["age_group"].isin(bands)]
            tb_bands = pr.merge(tb_bands, tb_population, on=["country", "year", "sex", "age_group"], how="left")
            grouped = tb_bands.groupby(keys, observed=True)[["number", "population"]]
            # count() ignores NaN, so requiring one contribution per band rejects any group where a band is
            # absent, reports no deaths, or has no recoverable population.
            tb_group = grouped.sum(min_count=1).where(grouped.count() == len(bands)).reset_index()
            tb_group["death_rate_per_100_000_population"] = tb_group["number"] / tb_group["population"] * 100_000
            tb_group["age_group"] = label
            tb_group = tb_group.drop(columns=["population"])
            _validate_derived_age_group(tb_group, label)
            parts.append(tb_group)

    tb_out = pr.concat(parts, ignore_index=True)
    # Concatenating a categorical with new labels gives plain strings; keep the dimensions categorical.
    for column in ["age_group", "cause"]:
        if isinstance(tb[column].dtype, pd.CategoricalDtype):
            metadata = tb_out[column].metadata
            tb_out[column] = tb_out[column].astype("category")
            tb_out[column].metadata = metadata  # ty: ignore
    return tb_out


def _implied_population(tb: Table) -> Table:
    """The population WHO divided by, per country, year, sex and age band, recovered from All Causes.

    WHO publishes a death rate next to every death count, so its denominator is `number / death_rate x
    100,000`. It describes the population, not the cause, and All Causes reports at least one death in nearly
    every cell, so reading it there recovers it almost everywhere; reading it from a rare cause would not.
    """
    tb_all = tb[tb["cause"] == "All Causes"][
        ["country", "year", "sex", "age_group", "number", "death_rate_per_100_000_population"]
    ]
    rate = tb_all["death_rate_per_100_000_population"]
    # A zero or missing rate carries no information about the population size.
    tb_all["population"] = tb_all["number"] / rate.where(rate > 0) * 100_000
    tb_all = tb_all.dropna(subset=["population"])[["country", "year", "sex", "age_group", "population"]]
    assert not tb_all.duplicated(subset=["country", "year", "sex", "age_group"]).any(), (
        "All Causes is not unique per cell"
    )
    return tb_all


def _validate_derived_age_group(tb_group: Table, label: str) -> None:
    """A rate of exactly zero must mean zero deaths, never an unknown denominator."""
    deaths, rate = tb_group["number"], tb_group["death_rate_per_100_000_population"]
    fabricated = tb_group[(rate == 0) & (deaths > 0)]
    assert len(fabricated) == 0, f"{label}: {len(fabricated)} rows report a death rate of 0 despite recording deaths"
    assert len(tb_group) > 0, f"{label}: no rows derived"
    log.info(
        "add_derived_age_groups",
        age_group=label,
        rows=len(tb_group),
        with_rate=int(rate.notna().sum()),
        without_rate=int(rate.isna().sum()),
    )


def build_most_common_cancer(tb: Table, tb_causes: Table) -> Table:
    """The cancer type with the highest age-standardized death rate, per country, year and sex.

    Age-standardized rates exist for all ages only, so the table has no age dimension. The cause is reported
    the way the step this replaces wrote it: without the word "cancer(s)", commas read as "and". Ties keep
    the first type in the data's order, as before; their number is logged.
    """
    rate = "age_standardized_death_rate_per_100_000_standard_population"
    cancers = set(tb_causes.loc[tb_causes["parent_cause"] == MALIGNANT, "cause"])
    tb_cancers = tb[tb["cause"].isin(cancers)]
    assert tb_cancers.loc[tb_cancers["age_group"] != "all ages", rate].isna().all(), (
        "Age-standardized rates outside all ages"
    )
    tb_rates = tb_cancers[tb_cancers["age_group"] == "all ages"].dropna(subset=[rate])[
        ["country", "year", "sex", "cause", rate]
    ]

    keys = ["country", "year", "sex"]
    tb_rates = tb_rates.reset_index(drop=True)
    maxima = tb_rates.groupby(keys, observed=True)[rate].transform("max")
    ties = int((tb_rates[rate] == maxima).groupby([tb_rates[k] for k in keys], observed=True).sum().gt(1).sum())
    tb_top = tb_rates.loc[tb_rates.groupby(keys, observed=True)[rate].idxmax(), keys + ["cause"]]
    log.info("build_most_common_cancer", rows=len(tb_top), cells_with_ties=ties)

    origins = tb_rates[rate].metadata.origins
    tb_top["cause"] = (
        tb_top["cause"]
        .astype(str)
        .str.replace(r"\bcancers?\b", "", case=False, regex=True)
        .str.replace(",", " and")
        .str.strip()
    )
    tb_top["cause"].metadata.origins = origins
    return tb_top.format(keys, short_name="most_common_cancer")


def _check_metadata_lists_every_cause(tb_causes: Table) -> None:
    """The metadata repeats two things per cause that only this step knows - its ICD-10 codes and its place in
    the tree - so a cause added here without its metadata branches fails the build instead of rendering blank."""
    text = Path(__file__).with_suffix(".meta.yml").read_text()
    missing = {
        "icd10_codes": [c for c in tb_causes["cause"] if f'cause == "{c}"' not in text],
        "cause_layer": [c for c in DRILL_DOWN_TABLES if f'("{c}", ' not in text],
    }
    assert not any(missing.values()), f"Causes missing from the metadata definitions: {missing}"


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

    # WHO's code strings space their commas and dashes unevenly ("D50-D53,D64.9", "J00-J22,  P23", "C77- C79"); normalize so they
    # read cleanly and match the strings repeated in the metadata.
    codes_metadata = tb_causes["icd10_codes"].metadata
    tb_causes["icd10_codes"] = (
        tb_causes["icd10_codes"]
        .astype(str)
        .str.replace(r"\s*,\s*", ", ", regex=True)
        .str.replace(r"\s*-\s*", "-", regex=True)
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
    tb_causes["table"] = [DRILL_DOWN_TABLES.get(cause, MAIN_TABLE) for cause in tb_causes["cause"]]
    # The tree is WHO's; the codes column already carries WHO's origin, so the new columns borrow it.
    for column in ["cause_level", "parent_cause", "table"]:
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
    min_year, max_year = 1950, date.today().year
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

    for cause, sex in SEX_SPECIFIC_CAUSES.items():
        cause_data = tb[(tb["cause"] == cause) & (tb["age_group"] == "all ages")]
        if len(cause_data) == 0:
            continue
        other_sex = "Females" if sex == "Males" else "Males"
        in_other_sex = cause_data.loc[cause_data["sex"] == other_sex, "number"].sum()
        total = cause_data.loc[cause_data["sex"] == "Both sexes", "number"].sum()
        share = float(in_other_sex) / float(total) if total else 0.0
        assert share <= MAX_SHARE_IN_OTHER_SEX, f"{cause}: {share:.2%} of deaths recorded in {other_sex.lower()}"
        if in_other_sex > 0:
            log.info(
                "Sex-specific cause with deaths recorded in the other sex",
                cause=cause,
                deaths=int(in_other_sex),
                share=f"{share:.4%}",
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
        # WHO's reporting lag is 18-24 months, so the last two data years are not expected of anyone.
        expected_years = set(range(2000, int(tb["year"].max()) - 1))
        missing_years = expected_years - years_available

        if len(missing_years) > 5:
            log.warning(
                "Significant temporal gaps",
                country=country,
                missing_years=len(missing_years),
                sample_missing=sorted(list(missing_years))[:5],
            )


def _ancestors(cause: str) -> list[str]:
    """The parents above a cause, nearest first, ending with All Causes."""
    tree = {**UNINGESTED_GROUPS, **CAUSE_TREE}
    chain = []
    parent = tree[cause][1]
    while parent is not None:
        chain.append(parent)
        parent = tree[parent][1]
    return chain


def _partition_groups(tb_causes: Table) -> list[str]:
    """The ingested groups that partition All Causes: those with no ingested ancestor below All Causes.

    A group with an ingested ancestor (measles under Infectious and parasitic diseases, a cancer type under
    Malignant neoplasms) is a drill-down: its deaths are already counted in that ancestor.
    """
    ingested = set(tb_causes["cause"])
    groups = sorted(
        cause
        for cause in ingested
        if cause != "All Causes" and not (set(_ancestors(cause)) - {"All Causes"}) & ingested
    )
    assert len(groups) == N_PARTITION_GROUPS, (
        f"Expected {N_PARTITION_GROUPS} partition groups, found {len(groups)}: {groups}"
    )
    return groups


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
