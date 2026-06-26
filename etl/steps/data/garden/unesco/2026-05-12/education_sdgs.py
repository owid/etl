"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder

log = get_logger()

# World source variants in priority order (UIS is most authoritative for UNESCO data)
_WORLD_VARIANTS = ["World (UIS)", "World (SDG)", "World (WB)", "World (UNICEF)", "World (MDG)"]
_WORLD_PRIORITY = {w: i for i, w in enumerate(_WORLD_VARIANTS)}

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Raw indicator_label_en values (before appending ", indicator_id") to keep in the final table.
# Everything else from the ~2900-indicator long table is filtered out BEFORE consolidate_world_entries
# and the pivot so that both operations are fast.
_VARIABLES_TO_KEEP = {
    # Adult literacy
    "Adult literacy rate, population 15+ years, both sexes (%)",
    "Adult literacy rate, population 15+ years, female (%)",
    "Adult literacy rate, population 15+ years, male (%)",
    # Youth literacy
    "Youth literacy rate, population 15-24 years, both sexes (%)",
    "Youth literacy rate, population 15-24 years, female (%)",
    "Youth literacy rate, population 15-24 years, male (%)",
    "Youth literacy rate, population 15-24 years, adjusted gender parity index (GPIA)",
    # Elderly literacy
    "Elderly literacy rate, population 65+ years, both sexes (%)",
    "Elderly literacy rate, population 65+ years, female (%)",
    "Elderly literacy rate, population 65+ years, male (%)",
    # Completion rates — modelled (both sexes, female, male)
    "Completion rate, primary education, both sexes (modelled data) (%)",
    "Completion rate, primary education, female (modelled data) (%)",
    "Completion rate, primary education, male (modelled data) (%)",
    "Completion rate, primary education, adjusted gender parity index (GPIA) (modelled data)",
    "Completion rate, lower secondary education, both sexes (modelled data) (%)",
    "Completion rate, lower secondary education, female (modelled data) (%)",
    "Completion rate, lower secondary education, male (modelled data) (%)",
    "Completion rate, lower secondary education, adjusted gender parity index (GPIA) (modelled data)",
    "Completion rate, upper secondary education, both sexes (modelled data) (%)",
    "Completion rate, upper secondary education, female (modelled data) (%)",
    "Completion rate, upper secondary education, male (modelled data) (%)",
    "Completion rate, upper secondary education, adjusted gender parity index (GPIA) (modelled data)",
    # Completion rates — observed (used in standalone charts)
    "Completion rate, primary education, both sexes (%)",
    "Completion rate, lower secondary education, both sexes (%)",
    "Completion rate, upper secondary education, both sexes (%)",
    "Completion rate, upper secondary education, female (%)",
    "Completion rate, upper secondary education, male (%)",
    # Educational attainment
    "Educational attainment rate, completed lower secondary education or higher, population 25+ years, both sexes (%)",
    "Educational attainment rate, completed post-secondary non-tertiary education or higher, population 25+ years, both sexes (%)",
    # Government expenditure on education as % of GDP
    "Government expenditure on education as a percentage of GDP (%)",
    # Government expenditure as % of total government expenditure
    "Expenditure on education as a percentage of total government expenditure (%) (UIS calculation)",
    # Per-student spending
    "Initial government funding per pre-primary student, constant PPP$",
    "Initial government funding per primary student, constant PPP$",
    "Initial government funding per lower secondary student, constant PPP$",
    "Initial government funding per upper secondary student, constant PPP$",
    "Initial government funding per tertiary student, constant PPP$",
    # Gross enrolment ratio — tertiary (used by OPRI combine_historical_enrollment)
    "Gross enrolment ratio for tertiary education, both sexes (%)",
    "Gross enrolment ratio for tertiary education, female (%)",
    "Gross enrolment ratio for tertiary education, male (%)",
    # Net enrolment rate — pre-primary
    "Net enrolment rate, pre-primary, both sexes (%)",
    "Net enrolment rate, pre-primary, female (%)",
    "Net enrolment rate, pre-primary, male (%)",
    # Out-of-school rates — observed (used by OPRI add_ner_from_oosr and MDims)
    "Out-of-school rate for children of primary school age, both sexes (%)",
    "Out-of-school rate for children of primary school age, female (%)",
    "Out-of-school rate for children of primary school age, male (%)",
    "Out-of-school rate for adolescents of lower secondary school age, both sexes (%)",
    "Out-of-school rate for adolescents of lower secondary school age, female (%)",
    "Out-of-school rate for adolescents of lower secondary school age, male (%)",
    "Out-of-school rate for youth of upper secondary school age, both sexes (%)",
    "Out-of-school rate for youth of upper secondary school age, female (%)",
    "Out-of-school rate for youth of upper secondary school age, male (%)",
    "Out-of-school rate for children one year before the official primary entry age, both sexes (%)",
    "Out-of-school rate for children one year before the official primary entry age, female (%)",
    "Out-of-school rate for children one year before the official primary entry age, male (%)",
    # Qualified teachers
    "Percentage of qualified teachers in pre-primary education, both sexes (%)",
    "Percentage of qualified teachers in primary education, both sexes (%)",
    "Percentage of qualified teachers in lower secondary education, both sexes (%)",
    "Percentage of qualified teachers in secondary education, both sexes (%)",
    "Percentage of qualified teachers in upper secondary education, both sexes (%)",
    # Pupil-teacher ratios
    "Pupil-qualified teacher ratio in pre-primary education (headcount basis)",
    "Pupil-qualified teacher ratio in primary education (headcount basis)",
    # School infrastructure
    "Proportion of primary schools with access to basic drinking water (%)",
    "Proportion of primary schools with single-sex basic sanitation facilities (%)",
    # Proficiency — prepared for the future
    "Proportion of children/young people at the age of primary education prepared for the future in mathematics, both sexes (%)",
    "Proportion of children/young people at the age of primary education prepared for the future in mathematics, female (%)",
    "Proportion of children/young people at the age of primary education prepared for the future in mathematics, male (%)",
    "Proportion of children/young people at the age of primary education prepared for the future in reading, both sexes (%)",
    "Proportion of children/young people at the age of primary education prepared for the future in reading, female (%)",
    "Proportion of children/young people at the age of primary education prepared for the future in reading, male (%)",
    "Proportion of children/young people at the age of lower secondary education prepared for the future in mathematics, both sexes (%)",
    "Proportion of children/young people at the age of lower secondary education prepared for the future in mathematics, female (%)",
    "Proportion of children/young people at the age of lower secondary education prepared for the future in mathematics, male (%)",
    "Proportion of children/young people at the age of lower secondary education prepared for the future in reading, both sexes (%)",
    "Proportion of children/young people at the age of lower secondary education prepared for the future in reading, female (%)",
    "Proportion of children/young people at the age of lower secondary education prepared for the future in reading, male (%)",
    # Proficiency — minimum proficiency level
    "Proportion of students at the end of primary education achieving at least a minimum proficiency level in reading, both sexes (%)",
    "Proportion of students at the end of primary education achieving at least a minimum proficiency level in reading, female (%)",
    "Proportion of students at the end of primary education achieving at least a minimum proficiency level in reading, male (%)",
    "Proportion of students at the end of primary education achieving at least a minimum proficiency level in mathematics, both sexes (%)",
    "Proportion of students at the end of primary education achieving at least a minimum proficiency level in mathematics, female (%)",
    "Proportion of students at the end of primary education achieving at least a minimum proficiency level in mathematics, male (%)",
    "Proportion of students at the end of lower secondary education achieving at least a minimum proficiency level in reading, both sexes (%)",
    "Proportion of students at the end of lower secondary education achieving at least a minimum proficiency level in reading, female (%)",
    "Proportion of students at the end of lower secondary education achieving at least a minimum proficiency level in reading, male (%)",
    "Proportion of students at the end of lower secondary education achieving at least a minimum proficiency level in mathematics, both sexes (%)",
    "Proportion of students at the end of lower secondary education achieving at least a minimum proficiency level in mathematics, female (%)",
    "Proportion of students at the end of lower secondary education achieving at least a minimum proficiency level in mathematics, male (%)",
    # Teacher salary (used in a standalone chart)
    "Average teacher salary in primary education relative to other professions requiring a comparable level of qualification, both sexes",
}

# Backward-compatible short_name renames: maps NEW short_name (from 2026 UNESCO labels) → OLD short_name.
# UNESCO changed the ordering or wording of tokens in some indicator labels between releases.
# Renaming preserves continuity for existing charts and MDim steps that reference the old short_names.
_SHORT_NAME_RENAMES = {
    # adjusted_attendance_rate → adjusted_net_attendance_rate
    "adjusted_net_attendance_rate__one_year_before_the_official_primary_entry_age__second_quintile__female__adjusted_location_parity_index__lpia__nara_agm1_q2_f_lpia": "adjusted_attendance_rate__one_year_before_the_official_primary_entry_age__second_quintile__female__adjusted_location_parity_index__lpia__nara_agm1_q2_f_lpia",
    "adjusted_net_attendance_rate__one_year_before_the_official_primary_entry_age__second_quintile__male__adjusted_location_parity_index__lpia__nara_agm1_q2_m_lpia": "adjusted_attendance_rate__one_year_before_the_official_primary_entry_age__second_quintile__male__adjusted_location_parity_index__lpia__nara_agm1_q2_m_lpia",
    # completion_rate: token order swapped __modelled_data__pct__ → __pct__modelled_data__
    "completion_rate__lower_secondary_education__both_sexes__modelled_data__pct__cr_mod_2": "completion_rate__lower_secondary_education__both_sexes__pct__modelled_data__cr_mod_2",
    "completion_rate__lower_secondary_education__female__modelled_data__pct__cr_mod_2_f": "completion_rate__lower_secondary_education__female__pct__modelled_data__cr_mod_2_f",
    "completion_rate__lower_secondary_education__male__modelled_data__pct__cr_mod_2_m": "completion_rate__lower_secondary_education__male__pct__modelled_data__cr_mod_2_m",
    "completion_rate__primary_education__both_sexes__modelled_data__pct__cr_mod_1": "completion_rate__primary_education__both_sexes__pct__modelled_data__cr_mod_1",
    "completion_rate__primary_education__female__modelled_data__pct__cr_mod_1_f": "completion_rate__primary_education__female__pct__modelled_data__cr_mod_1_f",
    "completion_rate__primary_education__male__modelled_data__pct__cr_mod_1_m": "completion_rate__primary_education__male__pct__modelled_data__cr_mod_1_m",
    "completion_rate__upper_secondary_education__both_sexes__modelled_data__pct__cr_mod_3": "completion_rate__upper_secondary_education__both_sexes__pct__modelled_data__cr_mod_3",
    "completion_rate__upper_secondary_education__female__modelled_data__pct__cr_mod_3_f": "completion_rate__upper_secondary_education__female__pct__modelled_data__cr_mod_3_f",
    "completion_rate__upper_secondary_education__male__modelled_data__pct__cr_mod_3_m": "completion_rate__upper_secondary_education__male__pct__modelled_data__cr_mod_3_m",
    # expenditure: UIS calculation qualifier added to indicator ID
    "expenditure_on_education_as_a_percentage_of_total_government_expenditure__pct__uis_calculation__xgovexp_imf": "expenditure_on_education_as_a_percentage_of_total_government_expenditure__pct__xgovexp_imf",
    # global citizenship: "national_education_policies" replaced by specific subcategory
    "extent_to_which__i__global_citizenship_education_and__ii__education_for_sustainable_development_are_mainstreamed_in_curricula__gcs_curricula": "extent_to_which__i__global_citizenship_education_and__ii__education_for_sustainable_development_are_mainstreamed_in_national_education_policies__gcs_curricula",
    "extent_to_which__i__global_citizenship_education_and__ii__education_for_sustainable_development_are_mainstreamed_in_student_assessment__gcs_studentassessment": "extent_to_which__i__global_citizenship_education_and__ii__education_for_sustainable_development_are_mainstreamed_in_national_education_policies__gcs_studentassessment",
    "extent_to_which__i__global_citizenship_education_and__ii__education_for_sustainable_development_are_mainstreamed_in_teacher_education__gcs_teachedu": "extent_to_which__i__global_citizenship_education_and__ii__education_for_sustainable_development_are_mainstreamed_in_national_education_policies__gcs_teachedu",
    # out_of_school_rate: token order swapped __modelled_data__pct__ → __pct__modelled_data__
    "out_of_school_rate_for_adolescents_of_lower_secondary_school_age__both_sexes__modelled_data__pct__rofst_mod_2": "out_of_school_rate_for_adolescents_of_lower_secondary_school_age__both_sexes__pct__modelled_data__rofst_mod_2",
    "out_of_school_rate_for_adolescents_of_lower_secondary_school_age__female__modelled_data__pct__rofst_mod_2_f": "out_of_school_rate_for_adolescents_of_lower_secondary_school_age__female__pct__modelled_data__rofst_mod_2_f",
    "out_of_school_rate_for_adolescents_of_lower_secondary_school_age__male__modelled_data__pct__rofst_mod_2_m": "out_of_school_rate_for_adolescents_of_lower_secondary_school_age__male__pct__modelled_data__rofst_mod_2_m",
    "out_of_school_rate_for_children_of_primary_school_age__both_sexes__modelled_data__pct__rofst_mod_1": "out_of_school_rate_for_children_of_primary_school_age__both_sexes__pct__modelled_data__rofst_mod_1",
    "out_of_school_rate_for_children_of_primary_school_age__female__modelled_data__pct__rofst_mod_1_f": "out_of_school_rate_for_children_of_primary_school_age__female__pct__modelled_data__rofst_mod_1_f",
    "out_of_school_rate_for_children_of_primary_school_age__male__modelled_data__pct__rofst_mod_1_m": "out_of_school_rate_for_children_of_primary_school_age__male__pct__modelled_data__rofst_mod_1_m",
    # rofst_agm1_gpia_cp: "official_age" → "official_primary_entry_age"
    "out_of_school_rate_for_children_one_year_before_the_official_primary_entry_age__adjusted_gender_parity_index__gpia__rofst_agm1_gpia_cp": "out_of_school_rate_for_children_one_year_before_the_official_age__adjusted_gender_parity_index__gpia__rofst_agm1_gpia_cp",
    "out_of_school_rate_for_youth_of_upper_secondary_school_age__both_sexes__modelled_data__pct__rofst_mod_3": "out_of_school_rate_for_youth_of_upper_secondary_school_age__both_sexes__pct__modelled_data__rofst_mod_3",
    "out_of_school_rate_for_youth_of_upper_secondary_school_age__female__modelled_data__pct__rofst_mod_3_f": "out_of_school_rate_for_youth_of_upper_secondary_school_age__female__pct__modelled_data__rofst_mod_3_f",
    "out_of_school_rate_for_youth_of_upper_secondary_school_age__male__modelled_data__pct__rofst_mod_3_m": "out_of_school_rate_for_youth_of_upper_secondary_school_age__male__pct__modelled_data__rofst_mod_3_m",
    # teacher professional development: "by_type_of_trained" → "by_type_of_training" (grammar fix)
    "percentage_of_teachers_in_lower_secondary_education_who_received_in_service_training_in_the_last_12_months_by_type_of_training__both_sexes__tprofd_2": "percentage_of_teachers_in_lower_secondary_education_who_received_in_service_training_in_the_last_12_months_by_type_of_trained__both_sexes__tprofd_2",
    "percentage_of_teachers_in_lower_secondary_education_who_received_in_service_training_in_the_last_12_months_by_type_of_training__female__tprofd_2_f": "percentage_of_teachers_in_lower_secondary_education_who_received_in_service_training_in_the_last_12_months_by_type_of_trained__female__tprofd_2_f",
    "percentage_of_teachers_in_lower_secondary_education_who_received_in_service_training_in_the_last_12_months_by_type_of_training__males__tprofd_2_m": "percentage_of_teachers_in_lower_secondary_education_who_received_in_service_training_in_the_last_12_months_by_type_of_trained__males__tprofd_2_m",
    "percentage_of_teachers_in_primary_education_who_received_in_service_training_in_the_last_12_months_by_type_of_training__both_sexes__tprofd_1": "percentage_of_teachers_in_primary_education_who_received_in_service_training_in_the_last_12_months_by_type_of_trained__both_sexes__tprofd_1",
    "percentage_of_teachers_in_primary_education_who_received_in_service_training_in_the_last_12_months_by_type_of_training__female__tprofd_1_f": "percentage_of_teachers_in_primary_education_who_received_in_service_training_in_the_last_12_months_by_type_of_trained__female__tprofd_1_f",
    "percentage_of_teachers_in_primary_education_who_received_in_service_training_in_the_last_12_months_by_type_of_training__male__tprofd_1_m": "percentage_of_teachers_in_primary_education_who_received_in_service_training_in_the_last_12_months_by_type_of_trained__male__tprofd_1_m",
}


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
        # Keep one row per group, preferring non-null values from the highest-priority source
        tb_to_consolidate["_priority"] = tb_to_consolidate["country"].map(_WORLD_PRIORITY).fillna(len(_WORLD_VARIANTS))
        tb_to_consolidate = tb_to_consolidate.sort_values(["_priority", "value"], na_position="last")
        tb_to_consolidate = tb_to_consolidate.drop_duplicates(subset=["year", "indicator_id"], keep="first")
        tb_to_consolidate = tb_to_consolidate.drop(columns=["_priority"])
        if hasattr(tb_to_consolidate["country"].dtype, "categories"):
            tb_to_consolidate["country"] = tb_to_consolidate["country"].astype(str)
        tb_to_consolidate["country"] = "World"

    parts = [p for p in [tb_other, tb_keep_as_is, tb_to_consolidate] if not p.empty]
    return pr.concat(parts) if len(parts) > 1 else parts[0]


def run() -> None:
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("education_sdgs")
    ds_expenditure = paths.load_dataset("public_expenditure")
    tb = ds_meadow.read("education_sdgs")
    tb_expenditure = ds_expenditure.read("public_expenditure")

    #
    # Process data.
    #
    country_mapping_path = paths.directory / "education.countries.json"
    excluded_countries_path = paths.directory / "education.excluded_countries.json"
    tb = paths.regions.harmonize_names(
        tb, country_col="country", countries_file=country_mapping_path, excluded_countries_file=excluded_countries_path
    )

    # Drop rows with missing indicator labels and strip whitespace from labels
    tb = tb[tb["indicator_label_en"].notna()]
    tb["indicator_label_en"] = tb["indicator_label_en"].str.strip()

    # Early filter: keep only the ~100 indicators we need.
    # This runs before consolidate_world_entries and the pivot, making both much faster.
    # check that all expected variables are present in the data:
    assert _VARIABLES_TO_KEEP.issubset(set(tb["indicator_label_en"].unique())), (
        "Some expected variables are missing from the data"
    )

    # keep only needed indicators:
    tb = tb[tb["indicator_label_en"].isin(_VARIABLES_TO_KEEP)]

    tb = consolidate_world_entries(tb)
    tb = tb.drop(columns=["magnitude", "qualifier"])

    # Append indicator_id to label to form unique column names (some labels exist for multiple IDs)
    tb["indicator_label_en"] = tb["indicator_label_en"].astype(str) + ", " + tb["indicator_id"].astype(str)
    # Pivot the table to have indicators as columns
    tb_pivoted = tb.pivot(index=["country", "year"], columns="indicator_label_en", values="value")
    tb_pivoted = tb_pivoted.reset_index()

    tb_pivoted = drop_outliers(tb_pivoted)

    tb_pivoted = tb_pivoted.format(["country", "year"])

    # Rename columns that UNESCO relabelled between releases, to preserve backward compatibility
    # with existing charts and MDim steps that reference the old short_names.
    existing_renames = {k: v for k, v in _SHORT_NAME_RENAMES.items() if k in tb_pivoted.columns}
    if existing_renames:
        tb_pivoted = tb_pivoted.rename(columns=existing_renames)

    # Combine recent expenditure data with historical estimates from a migrated dataset
    tb_pivoted = combine_historical_expenditure(tb_pivoted, tb_expenditure)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb_pivoted], default_metadata=ds_meadow.metadata)
    ds_garden.save()


def combine_historical_expenditure(tb: Table, tb_expenditure: Table) -> Table:
    """
    Merge historical and recent expenditure data into a single Table.

    This function combines data from a Table containing historical public expenditure on education
    with a primary Table. The function handles missing data by favoring recent data; if this is not available,
    it falls back to historical data, which could also be missing (NaN).

    """
    tb = tb.reset_index()

    # Historical expenditure data
    historic_expenditure = tb_expenditure[
        ["year", "country", "public_expenditure_on_education__tanzi__and__schuktnecht__2000"]
    ].copy()

    # Recent public expenditure from main table
    recent_expenditure = tb[
        ["year", "country", "government_expenditure_on_education_as_a_percentage_of_gdp__pct__xgdp_fsgov"]
    ].copy()

    # Merge historic and recent expenditure data based on 'year' and 'country'
    combined_df = pr.merge(historic_expenditure, recent_expenditure, on=["year", "country"], how="outer")

    # Combine expenditure data, favoring recent over historical.
    # Use .loc assignment to avoid unit-mismatch warnings from fillna across columns with
    # different unit metadata ('%' vs 'percent of GDP').
    combined_df["combined_expenditure_share_gdp"] = combined_df[
        "government_expenditure_on_education_as_a_percentage_of_gdp__pct__xgdp_fsgov"
    ].copy()
    mask = combined_df["combined_expenditure_share_gdp"].isna()
    combined_df.loc[mask, "combined_expenditure_share_gdp"] = combined_df.loc[
        mask, "public_expenditure_on_education__tanzi__and__schuktnecht__2000"
    ]
    combined_df["combined_expenditure_share_gdp"].metadata.unit = "%"
    combined_df["combined_expenditure_share_gdp"].metadata.short_unit = "%"
    combined_df[
        "combined_expenditure_share_gdp"
    ].metadata.title = "Public spending on education as a share of GDP (historical and recent)"

    # Merge the combined expenditure data back into the original table
    tb = pr.merge(
        tb,
        combined_df[["year", "country", "combined_expenditure_share_gdp"]],
        on=["year", "country"],
        how="outer",
    )

    tb = tb.format(["country", "year"])
    return tb


def drop_outliers(tb: Table) -> Table:
    """Remove implausible values from the pivoted SDGs table.

    Three categories of outliers are handled, each logged for auditability:

    1. **Zero-value expenditure (% of govt spending)**: UNESCO reports 0.0 for
       many country-years where data is simply missing. No country spends 0% of
       its budget on education; these are recording artifacts.

    2. **Zero-value percentage indicators**: Isolated 0.0 values in percentage
       columns (qualified teachers, school infrastructure) that are surrounded
       by plausible non-zero values. Treated as missing-reported-as-zero.

    3. **Specific point outliers**: Individual data points verified as source
       errors by comparing against surrounding years and cross-source checks.
    """
    n_total = 0

    # --- 1. Zero expenditure as % of govt spending ---
    # 541 rows across 104 countries where UNESCO reports exactly 0.0.
    # Countries like Australia (1980), Austria (1981-88), Argentina (1982-90)
    # all show 0% followed by normal 10-25% values — clearly missing data.
    # After pivot, column names are human-readable with indicator_id suffix, e.g.
    # "Expenditure on education as a percentage of total government expenditure (%) (UIS calculation), XGOVEXP.IMF"
    exp_govt_cols = [c for c in tb.columns if "percentage of total government expenditure" in c.lower()]

    for col in exp_govt_cols:
        if col in tb.columns:
            zero_mask = tb[col] == 0.0
            n = int(zero_mask.sum())
            if n > 0:
                tb.loc[zero_mask, col] = None
                n_total += n
                log.info(
                    "outlier_zeros_removed", indicator=col, n_removed=n, reason="0% govt expenditure is implausible"
                )

    # Also catch zeros in "percentage of GDP" expenditure columns (same logic).
    gdp_pct_cols = [c for c in tb.columns if "percentage of gdp" in c.lower() and "expenditure" in c.lower()]
    for col in gdp_pct_cols:
        zero_mask = tb[col] == 0.0
        n = int(zero_mask.sum())
        if n > 0:
            tb.loc[zero_mask, col] = None
            n_total += n
            log.info("outlier_zeros_removed", indicator=col, n_removed=n, reason="0% of GDP expenditure is implausible")

    # --- 2. Zero-value percentage indicators ---
    # Specific (country, year, column) triples where 0.0 is clearly missing data.
    # Each entry is documented with the surrounding context that confirms it's wrong.
    zero_outliers = [
        # Nauru: drinking water in primary schools was 100% for 2019-2023, then 0% in 2024.
        ("Nauru", 2024, "Proportion of primary schools with access to basic drinking water (%)"),
        # BVI: qualified teachers in primary was 78-100% for 2014-2021, then 0% in 2022.
        ("British Virgin Islands", 2022, "Percentage of qualified teachers in primary education, both sexes (%)"),
        # BVI: qualified teachers in secondary was ~82-100% for 2014-2021, then 0% in 2022.
        ("British Virgin Islands", 2022, "Percentage of qualified teachers in secondary education, both sexes (%)"),
    ]
    for country, year, indicator_label in zero_outliers:
        # After pivot, column names include ", indicator_id" suffix — match by prefix.
        matching_cols = [c for c in tb.columns if c.startswith(indicator_label)]
        for col in matching_cols:
            mask = (tb["country"] == country) & (tb["year"] == year) & (tb[col] == 0.0)
            n = int(mask.sum())
            if n > 0:
                tb.loc[mask, col] = None
                n_total += n
                log.info(
                    "outlier_removed",
                    country=country,
                    year=year,
                    indicator=col,
                    reason="0% is implausible given adjacent years",
                )

    # --- 3. Specific point outliers ---
    # Individual values confirmed as source errors.
    point_outliers = [
        # Algeria 2025: out-of-school rate jumped from ~4% to 77.6% for all sex breakdowns.
        # This is a single-year spike inconsistent with the entire 2011-2024 trend (3-7%).
        ("Algeria", 2025, "Out-of-school rate for adolescents of lower secondary school age"),
        # World 1970: tertiary GER = 17.44, but 1971 = 8.73 and Lee & Lee 1970 = 9.49.
        # Isolated spike in early UNESCO data; surrounding years are 8-11%.
        ("World", 1970, "Gross enrolment ratio for tertiary education"),
    ]
    for country, year, indicator_prefix in point_outliers:
        matching_cols = [c for c in tb.columns if c.startswith(indicator_prefix)]
        for col in matching_cols:
            mask = (tb["country"] == country) & (tb["year"] == year) & tb[col].notna()
            n = int(mask.sum())
            if n > 0:
                tb.loc[mask, col] = None
                n_total += n
                log.info(
                    "outlier_removed",
                    country=country,
                    year=year,
                    indicator=col,
                    reason="point outlier inconsistent with adjacent years",
                )

    log.info("outliers_summary", step="education_sdgs", total_cells_removed=n_total)
    return tb
