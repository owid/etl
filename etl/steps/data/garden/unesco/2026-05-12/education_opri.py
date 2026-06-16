"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder

# World source variants as they appear after country harmonization
_WORLD_VARIANTS = ["World (UIS)", "World (SDG)", "World (WB)", "World (UNICEF)", "World (MDG)"]

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def _world_values_agree(series) -> bool:
    """Return True if all non-null values in a group agree within 1% relative tolerance."""
    vals = series.dropna()
    if len(vals) <= 1:
        return True
    mean = vals.mean()
    if mean == 0:
        return (vals.max() - vals.min()) == 0
    return (vals.max() - vals.min()) / abs(mean) <= 0.01


def consolidate_world_entries(tb: Table) -> Table:
    """Replace multiple World source variants with a single 'World' row per year+indicator.

    Consolidates when all non-null values within a (year, indicator_id) group agree within
    1% relative tolerance. Groups with genuinely different values are left unchanged.
    The first non-null row (or first row if all null) is kept as the 'World' entry.
    """
    world_mask = tb["country"].isin(_WORLD_VARIANTS)
    if not world_mask.any():
        return tb

    tb_world = tb[world_mask]
    tb_other = tb[~world_mask]

    # Identify (year, indicator_id) groups where all sources report the same value
    can_consolidate = set(
        tb_world.groupby(["year", "indicator_id"])["value"].apply(_world_values_agree).loc[lambda s: s].index.tolist()
    )

    # Tag rows belonging to consolidatable groups
    consolidate_keys = {f"{y}___{i}" for y, i in can_consolidate}
    tb_world = tb_world.copy()
    tb_world["_consolidate"] = [
        f"{y}___{i}" in consolidate_keys for y, i in zip(tb_world["year"].values, tb_world["indicator_id"].values)
    ]

    tb_to_consolidate = tb_world[tb_world["_consolidate"]].drop(columns=["_consolidate"]).copy()
    tb_keep_as_is = tb_world[~tb_world["_consolidate"]].drop(columns=["_consolidate"])

    if not tb_to_consolidate.empty:
        # Keep one row per group, preferring non-null values
        tb_to_consolidate = tb_to_consolidate.sort_values("value", na_position="last")
        tb_to_consolidate = tb_to_consolidate.drop_duplicates(subset=["year", "indicator_id"], keep="first")
        if hasattr(tb_to_consolidate["country"].dtype, "categories"):
            tb_to_consolidate["country"] = tb_to_consolidate["country"].astype(str)
        tb_to_consolidate["country"] = "World"

    parts = [p for p in [tb_other, tb_keep_as_is, tb_to_consolidate] if not p.empty]
    return pr.concat(parts) if len(parts) > 1 else parts[0]


# Variables to keep in the final table — the ~80 OPRI indicators with defined metadata
# in education_opri.meta.yml that are used in MDims or standalone charts.
# Everything else from the ~400-indicator long table is filtered out BEFORE pivoting
# so that the pivot, consolidate_world_entries, and feather write are all fast.
_VARIABLES_TO_KEEP = [
    # Out-of-school children (numbers)
    "Out-of-school children of primary school age, both sexes (number)",
    "Out-of-school children of primary school age, female (number)",
    "Out-of-school children of primary school age, male (number)",
    "Out-of-school adolescents of lower secondary school age, both sexes (number)",
    "Out-of-school adolescents of lower secondary school age, female (number)",
    "Out-of-school adolescents of lower secondary school age, male (number)",
    "Out-of-school children, one year before the official primary entry age, both sexes (number)",
    "Out-of-school children, one year before the official primary entry age, female (number)",
    "Out-of-school children, one year before the official primary entry age, male (number)",
    "Out-of-school youth of upper secondary school age, both sexes (number)",
    "Out-of-school youth of upper secondary school age, female (number)",
    "Out-of-school youth of upper secondary school age, male (number)",
    # Mean years of schooling
    "Mean years of schooling (ISCED 1 or higher), population 25+ years, both sexes",
    "Mean years of schooling (ISCED 1 or higher), population 25+ years, female",
    "Mean years of schooling (ISCED 1 or higher), population 25+ years, male",
    "Mean years of schooling (ISCED 1 or higher), population 25+ years, adjusted gender parity index (GPIA)",
    # Private institutions
    "Percentage of enrolment in pre-primary education in private institutions, both sexes (%)",
    "Percentage of enrolment in primary education in private institutions, both sexes (%)",
    # Attendance rates
    "Total net attendance rate, primary, both sexes (%)",
    # Mobility
    "Outbound mobility ratio, all regions, both sexes (UIS estimate) (%)",
    "Inbound mobility rate, both sexes (UIS estimate) (%)",
    # Education system structure
    "Official entrance age to compulsory education (years)",
    "Duration of compulsory education (years)",
    "Official entrance age to pre-primary education (years)",
    "Theoretical duration of pre-primary education (years)",
    # NER gender parity indices
    "Total net enrolment rate, primary, adjusted gender parity index (GPIA)",
    "Total net enrolment rate, lower secondary, adjusted gender parity index (GPIA)",
    # Staff compensation
    "All staff compensation as a percentage of total expenditure in public institutions (%)",
    "All staff compensation as a percentage of total expenditure in primary public institutions (%)",
    # Female teachers
    "Percentage of teachers in primary education who are female (%)",
    "Percentage of teachers in secondary education who are female (%)",
    "Percentage of teachers in tertiary education who are female (%)",
    # Net enrolment rates
    "Total net enrolment rate, primary, both sexes (%)",
    "Total net enrolment rate, primary, male (%)",
    "Total net enrolment rate, primary, female (%)",
    "Total net enrolment rate, lower secondary, both sexes (%)",
    "Total net enrolment rate, lower secondary, male (%)",
    "Total net enrolment rate, lower secondary, female (%)",
    "Total net enrolment rate, upper secondary, both sexes (%)",
    "Total net enrolment rate, upper secondary, male (%)",
    "Total net enrolment rate, upper secondary, female (%)",
    # Gross enrolment ratios
    "Gross enrolment ratio, primary, both sexes (%)",
    "Gross enrolment ratio, primary, male (%)",
    "Gross enrolment ratio, primary, female (%)",
    "Gross enrolment ratio, lower secondary, both sexes (%)",
    "Gross enrolment ratio, lower secondary, male (%)",
    "Gross enrolment ratio, lower secondary, female (%)",
    "Gross enrolment ratio, upper secondary, both sexes (%)",
    "Gross enrolment ratio, upper secondary, female (%)",
    "Gross enrolment ratio, upper secondary, male (%)",
    "Gross enrolment ratio, secondary, both sexes (%)",
    # School life expectancy
    "School life expectancy, pre-primary, both sexes (years)",
    "School life expectancy, pre-primary, female (years)",
    "School life expectancy, pre-primary, male (years)",
    "School life expectancy, primary, both sexes (years)",
    "School life expectancy, primary, female (years)",
    "School life expectancy, primary, male (years)",
    "School life expectancy, secondary, both sexes (years)",
    "School life expectancy, secondary, female (years)",
    "School life expectancy, secondary, male (years)",
    "School life expectancy, tertiary, both sexes (years)",
    "School life expectancy, tertiary, female (years)",
    "School life expectancy, tertiary, male (years)",
    "School life expectancy, primary, adjusted gender parity index (GPIA)",
    # Government expenditure as % of GDP
    "Government expenditure on pre-primary education as a percentage of GDP (%)",
    "Government expenditure on primary education as a percentage of GDP (%)",
    "Government expenditure on lower secondary education as a percentage of GDP (%)",
    "Government expenditure on upper secondary education as a percentage of GDP (%)",
    "Government expenditure on secondary education as a percentage of GDP (%)",
    "Government expenditure on tertiary education as a percentage of GDP (%)",
    # Government expenditure in constant PPP$
    "Government expenditure on pre-primary education, constant PPP$ (millions)",
    "Government expenditure on primary education, constant PPP$ (millions)",
    "Government expenditure on lower secondary education, constant PPP$ (millions)",
    "Government expenditure on upper secondary education, constant PPP$ (millions)",
    "Government expenditure on tertiary education, constant PPP$ (millions)",
    "Government expenditure on education, constant PPP$ (millions)",
    "Government expenditure on education, PPP$ (millions)",
    # Derived: per-student spending (computed in run())
    "Government expenditure on education per student, total across all levels (constant PPP$)",
    # Combined historical enrollment series (added by combine_historical_enrollment)
    "Net enrolment rate, primary, both sexes (%), combined historical",
    "Net enrolment rate, primary, female (%), combined historical",
    "Net enrolment rate, primary, male (%), combined historical",
    "Gross enrolment ratio, tertiary, both sexes (%), combined historical",
    "Gross enrolment ratio, tertiary, female (%), combined historical",
    "Gross enrolment ratio, tertiary, male (%), combined historical",
]

# Enrollment indicator names needed transiently to compute per-student spending.
# These are included in the pivot but dropped from the final table after the derived
# column is calculated.
_ENROLLMENT_FOR_DERIVED = [
    "Enrolment in pre-primary education, both sexes (number)",
    "Enrolment in primary education, both sexes (number)",
    "Enrolment in lower secondary education, both sexes (number)",
    "Enrolment in upper secondary education, both sexes (number)",
    "Enrolment in tertiary education, all programmes, both sexes (number)",
]

# Maps Lee & Lee pivoted enrollment column names → OPRI-style human-readable column names.
# Secondary enrollment is omitted (split lower/upper in OPRI; combined not available without WB WDI).
_COMBINED_ENROLMENT_TO_OPRI = {
    "mf_primary_enrollment_rates": "Net enrolment rate, primary, both sexes (%), combined historical",
    "f_primary_enrollment_rates": "Net enrolment rate, primary, female (%), combined historical",
    "m_primary_enrollment_rates": "Net enrolment rate, primary, male (%), combined historical",
    "mf_tertiary_enrollment_rates": "Gross enrolment ratio, tertiary, both sexes (%), combined historical",
    "f_tertiary_enrollment_rates": "Gross enrolment ratio, tertiary, female (%), combined historical",
    "m_tertiary_enrollment_rates": "Gross enrolment ratio, tertiary, male (%), combined historical",
}

# SDGs short_names for tertiary GER (used as the recent source for the combined historical series)
_SDG_TERTIARY_COLS = {
    "gross_enrolment_ratio_for_tertiary_education__both_sexes__pct__ger_5t8": "mf_tertiary_enrollment_rates",
    "gross_enrolment_ratio_for_tertiary_education__female__pct__ger_5t8_f": "f_tertiary_enrollment_rates",
    "gross_enrolment_ratio_for_tertiary_education__male__pct__ger_5t8_m": "m_tertiary_enrollment_rates",
}


def combine_historical_enrollment(tb_opri: Table, tb_lee: Table, tb_sdgs: Table) -> Table:
    """Combine Lee & Lee historical enrollment (pre-1985) with recent OPRI and SDGs data (1985+).

    The combined series uses:
    - Primary NER (1985+): UNESCO OPRI direct survey data
    - Tertiary GER (1985+): UNESCO SDGs dataset (GER.5T8)
    - Primary and tertiary (pre-1985): Lee & Lee (2016) historical estimates

    Both primary and tertiary use 1985 as the cutoff year between Lee & Lee
    historical estimates and UNESCO recent data.

    Secondary enrollment is omitted because OPRI and SDGs only have separate
    lower/upper secondary series, not a single combined secondary series.

    Returns the OPRI table with combined enrollment columns appended.
    Individual country series go back to ~1820; World and regional aggregates
    are included for all periods via Lee & Lee garden aggregates (pre-1985)
    and OPRI/SDGs (1985+).
    """

    # Pre-1985 historical data from Lee & Lee for both primary and tertiary.
    # Recent data (1985+) comes from UNESCO OPRI (primary) and SDGs (tertiary).
    tb_hist = tb_lee[tb_lee["year"] < 1985].copy()

    # 3. Recent primary NER (1985+) from OPRI itself
    opri_primary_map = {
        "Total net enrolment rate, primary, both sexes (%)": "mf_primary_enrollment_rates",
        "Total net enrolment rate, primary, female (%)": "f_primary_enrollment_rates",
        "Total net enrolment rate, primary, male (%)": "m_primary_enrollment_rates",
    }
    avail_primary = {k: v for k, v in opri_primary_map.items() if k in tb_opri.columns}
    tb_primary = (
        tb_opri[["country", "year"] + list(avail_primary)]
        .rename(columns=avail_primary)
        .loc[lambda t: t["year"] >= 1985]
        .copy()
    )

    # 4. Recent tertiary GER (1985+) from SDGs (world aggregates available from 1985)
    avail_tertiary = {k: v for k, v in _SDG_TERTIARY_COLS.items() if k in tb_sdgs.columns}
    tb_tertiary = (
        tb_sdgs[["country", "year"] + list(avail_tertiary)]
        .rename(columns=avail_tertiary)
        .loc[lambda t: t["year"] >= 1985]
        .copy()
    )

    # 5. Merge recent primary + tertiary into one recent table
    tb_recent = pr.merge(tb_primary, tb_tertiary, on=["country", "year"], how="outer")

    # 6. Combine historical (Lee & Lee, pre-1985) and recent (OPRI/SDG, 1985+) using outer merge.
    # The cutoff is 1985 for both primary and tertiary; prefer the recent source,
    # falling back to Lee & Lee where the recent source has no data.
    tb_combined = pr.merge(tb_recent, tb_hist, on=["country", "year"], how="outer", suffixes=("", "_hist"))

    hist_enr_cols = [c for c in tb_hist.columns if c not in ["country", "year"]]
    for col in hist_enr_cols:
        hist_col = f"{col}_hist"
        if hist_col in tb_combined.columns:
            mask = tb_combined[col].isna() & tb_combined[hist_col].notna()
            tb_combined.loc[mask, col] = tb_combined.loc[mask, hist_col]
            tb_combined = tb_combined.drop(columns=[hist_col])

    # 7. Known data error: US 1975 male primary rate is implausible
    mask_us75 = (tb_combined["country"] == "United States") & (tb_combined["year"] == 1975)
    if mask_us75.any() and "m_primary_enrollment_rates" in tb_combined.columns:
        tb_combined.loc[mask_us75, "m_primary_enrollment_rates"] = None

    # 8. Rename to OPRI-style human-readable column names
    rename = {k: v for k, v in _COMBINED_ENROLMENT_TO_OPRI.items() if k in tb_combined.columns}
    tb_combined = tb_combined.rename(columns=rename)

    paths.log.info(
        "combined_enrollment_added",
        n_countries=tb_combined["country"].nunique(),
        earliest_year=int(tb_combined["year"].min()),
        n_rows=len(tb_combined),
    )

    # 10. Outer-merge onto OPRI: fills in enrollment for existing (country, year) pairs
    #     and adds new rows for pre-1985 historical entries not in OPRI.
    # Only include the renamed combined columns (not raw Lee & Lee secondary rates).
    combined_cols = list(_COMBINED_ENROLMENT_TO_OPRI.values())
    new_cols = [c for c in tb_combined.columns if c in combined_cols]
    return pr.merge(tb_opri, tb_combined[["country", "year"] + new_cols], on=["country", "year"], how="outer")


# Maps OOSR column names (from education_sdgs) → NER column names (in education_opri).
# Keys are SDGs underscore short_names (columns of tb_sdgs after reset_index()).
# Values are the human-readable OPRI column names (columns of tb_opri after pivot).
# NER ≈ 100 - OOSR holds as an identity for countries that have direct UNESCO survey data.
# For regional aggregates not covered by OPRI, we derive NER using this relationship.
_OOSR_TO_NER = {
    "out_of_school_rate_for_children_of_primary_school_age__both_sexes__pct__rofst_1_cp": "Total net enrolment rate, primary, both sexes (%)",
    "out_of_school_rate_for_children_of_primary_school_age__female__pct__rofst_1_f_cp": "Total net enrolment rate, primary, female (%)",
    "out_of_school_rate_for_children_of_primary_school_age__male__pct__rofst_1_m_cp": "Total net enrolment rate, primary, male (%)",
    "out_of_school_rate_for_adolescents_of_lower_secondary_school_age__both_sexes__pct__rofst_2_cp": "Total net enrolment rate, lower secondary, both sexes (%)",
    "out_of_school_rate_for_adolescents_of_lower_secondary_school_age__female__pct__rofst_2_f_cp": "Total net enrolment rate, lower secondary, female (%)",
    "out_of_school_rate_for_adolescents_of_lower_secondary_school_age__male__pct__rofst_2_m_cp": "Total net enrolment rate, lower secondary, male (%)",
    "out_of_school_rate_for_youth_of_upper_secondary_school_age__both_sexes__pct__rofst_3_cp": "Total net enrolment rate, upper secondary, both sexes (%)",
    "out_of_school_rate_for_youth_of_upper_secondary_school_age__female__pct__rofst_3_f_cp": "Total net enrolment rate, upper secondary, female (%)",
    "out_of_school_rate_for_youth_of_upper_secondary_school_age__male__pct__rofst_3_m_cp": "Total net enrolment rate, upper secondary, male (%)",
}


def add_ner_from_oosr(tb_opri: Table, tb_sdgs: Table) -> Table:
    """Add NER = 100 - OOSR for aggregates that have OOSR in SDGs but no NER in OPRI.

    Regional aggregates (e.g. 'Central and Southern Asia (SDG)', income groups, World)
    have OOSR data in the UNESCO SDG dataset but are not covered by OPRI.
    This function derives their NER using the identity NER = 100 - OOSR and appends
    those rows to the OPRI table. Aggregates that already have NER in OPRI are skipped.
    """
    sdgs_oosr_cols = [col for col in _OOSR_TO_NER if col in tb_sdgs.columns]
    if not sdgs_oosr_cols:
        paths.log.warning("ner_from_oosr_skipped", note="No OOSR columns found in SDGs dataset")
        return tb_opri

    sdgs_sub = tb_sdgs[["country", "year"] + sdgs_oosr_cols].copy()

    # Compute NER = 100 - OOSR for each education level / sex combination
    ner_cols = []
    for oosr_col in sdgs_oosr_cols:
        ner_col = _OOSR_TO_NER[oosr_col]
        sdgs_sub[ner_col] = 100 - sdgs_sub[oosr_col]
        ner_cols.append(ner_col)

    sdgs_sub = sdgs_sub.drop(columns=sdgs_oosr_cols)
    sdgs_sub = sdgs_sub.dropna(subset=ner_cols, how="all")

    # Identify countries that already have NER data in OPRI (skip them)
    opri_ner_cols = [col for col in ner_cols if col in tb_opri.columns]
    if opri_ner_cols:
        countries_with_ner = set(tb_opri.loc[tb_opri[opri_ner_cols].notna().any(axis=1), "country"].unique())
    else:
        countries_with_ner = set()

    sdgs_new = sdgs_sub[~sdgs_sub["country"].isin(countries_with_ner)].copy()

    if sdgs_new.empty:
        paths.log.info("ner_from_oosr", note="No new NER rows to add from SDGs")
        return tb_opri

    paths.log.info(
        "ner_from_oosr_added",
        n_countries=sdgs_new["country"].nunique(),
        n_rows=len(sdgs_new),
        countries=sorted(sdgs_new["country"].astype(str).unique().tolist()),
    )

    # Use an outer merge instead of concat to avoid duplicate (country, year) rows.
    # SDG regions may already have rows in OPRI for non-NER indicators; we want to
    # fill NER into those existing rows, and only add truly new rows for (country, year)
    # pairs not present in OPRI at all.
    sdgs_ner_df = sdgs_new[["country", "year"] + ner_cols]
    result = pr.merge(tb_opri, sdgs_ner_df, on=["country", "year"], how="outer", suffixes=("", "_sdgs"))

    for ner_col in ner_cols:
        sdgs_col = f"{ner_col}_sdgs"
        if sdgs_col in result.columns:
            mask = result[ner_col].isna() & result[sdgs_col].notna()
            result.loc[mask, ner_col] = result.loc[mask, sdgs_col]
            result = result.drop(columns=[sdgs_col])

    return result


def _drop_if_abnormal(tb, country, year, col, threshold, reason):
    """Null out a single cell if it exists, exceeds `threshold`, and log the removal.

    If the value is present but NOT above the threshold the data may have been fixed
    upstream; a warning is logged so the filter can be reviewed.
    """
    if col not in tb.columns:
        return
    mask = (tb["country"] == country) & (tb["year"] == year)
    val = tb.loc[mask, col]
    if val.empty or val.isna().all():
        return
    numeric_val = float(val.iloc[0])
    if numeric_val > threshold:
        tb.loc[mask, col] = None
        paths.log.info(
            "outlier_removed",
            country=country,
            year=year,
            indicator=col,
            value=round(numeric_val, 4),
            reason=reason,
        )
    else:
        paths.log.warning(
            "outlier_check_skipped",
            country=country,
            year=year,
            indicator=col,
            value=round(numeric_val, 4),
            note="Value is no longer above threshold — upstream data may have changed. Review this filter.",
        )


def drop_outliers(tb):
    """Remove known impossible values from the pivoted OPRI table and log each removal.

    Each entry is checked against a plausibility threshold before removal so that if
    the source data is corrected the filter raises a warning instead of silently no-oping.
    """
    # Sierra Leone 2023: multiple education-level GDP columns report >100 % of GDP (impossible).
    sl_cols = [
        "Government expenditure on primary education as a percentage of GDP (%)",
        "Government expenditure on lower secondary education as a percentage of GDP (%)",
        "Government expenditure on upper secondary education as a percentage of GDP (%)",
        "Government expenditure on tertiary education as a percentage of GDP (%)",
        "Government expenditure on secondary education as a percentage of GDP (%)",
        "Government expenditure on post-secondary non-tertiary education as a percentage of GDP (%)",
    ]
    for col in sl_cols:
        _drop_if_abnormal(
            tb,
            "Sierra Leone",
            2023,
            col,
            threshold=10,
            reason="value exceeds 10 % of GDP — physically impossible for a single education level",
        )

    # Chad 2024: primary education spending reported as ~177 % of GDP.
    _drop_if_abnormal(
        tb,
        "Chad",
        2024,
        "Government expenditure on primary education as a percentage of GDP (%)",
        threshold=10,
        reason="value of ~177 % of GDP is physically impossible",
    )

    # Oman 2017 / 2019 / 2021: alternating-year spikes (12–165 % GDP) across all
    # level-specific columns while adjacent even years are normal (~0–2 % GDP).
    oman_cols = [
        "Government expenditure on pre-primary education as a percentage of GDP (%)",
        "Government expenditure on primary education as a percentage of GDP (%)",
        "Government expenditure on lower secondary education as a percentage of GDP (%)",
        "Government expenditure on secondary education as a percentage of GDP (%)",
        "Government expenditure on upper secondary education as a percentage of GDP (%)",
    ]
    for year in [2017, 2019, 2021]:
        for col in oman_cols:
            _drop_if_abnormal(
                tb,
                "Oman",
                year,
                col,
                threshold=10,
                reason=f"alternating-year spike pattern ({year}): value far exceeds normal range of ~0–2 % GDP",
            )

    return tb


def run() -> None:
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("education_opri")
    tb = ds_meadow.read("education_opri")

    # Load SDGs garden dataset to derive NER for regional aggregates
    ds_sdgs = paths.load_dataset("education_sdgs")
    tb_sdgs = ds_sdgs.read("education_sdgs")

    # Load Lee & Lee garden enrollment table (pre-2000 historical, includes regional aggregates)
    ds_lee = paths.load_dataset("education_lee_lee")
    tb_lee_enrollment = ds_lee.read("education_lee_lee_enrollment")

    #
    # Process data.
    #
    country_mapping_path = paths.directory / "education.countries.json"
    excluded_countries_path = paths.directory / "education.excluded_countries.json"
    tb = paths.regions.harmonize_names(
        tb, country_col="country", countries_file=country_mapping_path, excluded_countries_file=excluded_countries_path
    )

    # Drop rows where the indicator has no label (new indicators not yet in label file)
    tb = tb.dropna(subset=["indicator_label_en"])

    # Early filter: keep only the ~80 indicators we need + the enrollment counts needed
    # to compute per-student spending. This runs before consolidate_world_entries and
    # the pivot, making both operations much faster.

    # assert set(_VARIABLES_TO_KEEP).issubset(set(tb["indicator_label_en"].unique())), (
    #    f"Some expected variables are missing from the data: {set(_VARIABLES_TO_KEEP) - set(tb['indicator_label_en'].unique())}"
    # )

    assert set(_ENROLLMENT_FOR_DERIVED).issubset(set(tb["indicator_label_en"].unique())), (
        f"Some enrollment variables needed for derived computations are missing from the data: {set(_ENROLLMENT_FOR_DERIVED) - set(tb['indicator_label_en'].unique())}"
    )

    _vars_to_pivot = set(_VARIABLES_TO_KEEP) | set(_ENROLLMENT_FOR_DERIVED)
    tb = tb[tb["indicator_label_en"].isin(_vars_to_pivot)]

    tb = consolidate_world_entries(tb)
    tb = tb.drop(columns=["indicator_id", "magnitude", "qualifier"])

    # Pivot the table to have indicators as columns
    tb_pivoted = tb.pivot(index=["country", "year"], columns="indicator_label_en", values="value")

    # Scale expenditure columns from millions to full values
    millions_cols = [col for col in tb_pivoted.columns if "constant PPP$ (millions)" in col]
    tb_pivoted[millions_cols] = tb_pivoted[millions_cols] * 1_000_000

    # Compute per-student spending (needs enrollment counts from _ENROLLMENT_FOR_DERIVED)
    expenditure_enrollment_mapping = {
        "Government expenditure on pre-primary education, constant PPP$ (millions)": "Enrolment in pre-primary education, both sexes (number)",
        "Government expenditure on primary education, constant PPP$ (millions)": "Enrolment in primary education, both sexes (number)",
        "Government expenditure on lower secondary education, constant PPP$ (millions)": "Enrolment in lower secondary education, both sexes (number)",
        "Government expenditure on upper secondary education, constant PPP$ (millions)": "Enrolment in upper secondary education, both sexes (number)",
        "Government expenditure on tertiary education, constant PPP$ (millions)": "Enrolment in tertiary education, all programmes, both sexes (number)",
    }
    expenditure_cols = [col for col in expenditure_enrollment_mapping if col in tb_pivoted.columns]
    enrollment_cols = [
        v for k, v in expenditure_enrollment_mapping.items() if k in tb_pivoted.columns and v in tb_pivoted.columns
    ]
    if expenditure_cols and enrollment_cols:
        tb_pivoted["Enrolment in education, total across all levels (number)"] = tb_pivoted[enrollment_cols].sum(
            axis=1, skipna=False
        )
        total_exp_col = "Government expenditure on education, constant PPP$ (millions)"
        total_enrol_col = "Enrolment in education, total across all levels (number)"
        if total_exp_col in tb_pivoted.columns:
            tb_pivoted["Government expenditure on education per student, total across all levels (constant PPP$)"] = (
                tb_pivoted[total_exp_col] / tb_pivoted[total_enrol_col].replace(0, None)
            )

    # Drop the temporary enrollment columns (only needed for the derived computation above)
    cols_to_drop = _ENROLLMENT_FOR_DERIVED + ["Enrolment in education, total across all levels (number)"]
    tb_pivoted = tb_pivoted.drop(columns=[c for c in cols_to_drop if c in tb_pivoted.columns])

    tb_pivoted = tb_pivoted.reset_index()

    tb_pivoted = drop_outliers(tb_pivoted)
    tb_pivoted = add_ner_from_oosr(tb_pivoted, tb_sdgs)
    tb_pivoted = combine_historical_enrollment(tb_pivoted, tb_lee_enrollment, tb_sdgs)

    tb_pivoted = tb_pivoted.format(["country", "year"])

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb_pivoted], default_metadata=ds_meadow.metadata)
    ds_garden.save()
