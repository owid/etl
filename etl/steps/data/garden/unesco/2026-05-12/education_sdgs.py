"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table, VariableMeta

from etl.helpers import PathFinder

# World source variants in priority order (UIS is most authoritative for UNESCO data)
_WORLD_VARIANTS = ["World (UIS)", "World (SDG)", "World (WB)", "World (UNICEF)", "World (MDG)"]
_WORLD_PRIORITY = {w: i for i, w in enumerate(_WORLD_VARIANTS)}

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Curated titles for indicators that have human-readable labels different from the raw UNESCO label.
# Keys are the raw indicator labels as they appear as column names after the pivot
# (label + ", " + indicator_id).
_TITLE_OVERRIDES = {
    "Adult literacy rate, population 15+ years, both sexes (%), LR.AG15T99": "Literacy rate among adults",
    "Adult literacy rate, population 15+ years, female (%), LR.AG15T99.F": "Literacy rate among adult women",
    "Adult literacy rate, population 15+ years, male (%), LR.AG15T99.M": "Literacy rate among adult men",
    "Completion rate, lower secondary education, adjusted gender parity index (GPIA) (modelled data), CR.MOD.2.GPIA": "Completion rate in lower secondary education, adjusted gender parity index (GPIA)",
    "Completion rate, primary education, adjusted gender parity index (GPIA) (modelled data), CR.MOD.1.GPIA": "Completion rate in primary education, adjusted gender parity index (GPIA)",
    "Completion rate, upper secondary education, adjusted gender parity index (GPIA) (modelled data), CR.MOD.3.GPIA": "Completion rate in upper secondary education, adjusted gender parity index (GPIA)",
    "Educational attainment rate, completed lower secondary education or higher, population 25+ years, both sexes (%), EA.2T8.AG25T99": "Share of the population with lower secondary education (25+)",
    "Educational attainment rate, completed post-secondary non-tertiary education or higher, population 25+ years, both sexes (%), EA.4T8.AG25T99": "Share of the population with post-secondary education or higher (25+)",
    "Elderly literacy rate, population 65+ years, both sexes (%), LR.AG65T99": "Literacy rate among older adults",
    "Elderly literacy rate, population 65+ years, female (%), LR.AG65T99.F": "Literacy rate among older adult women",
    "Elderly literacy rate, population 65+ years, male (%), LR.AG65T99.M": "Literacy rate among older adult men",
    "Government expenditure on education as a percentage of GDP (%), XGDP.FSGOV": "Government spending on education as share of GDP",
    "Gross enrolment ratio for tertiary education, both sexes (%), GER.5T8": "Gross enrollment ratio in tertiary education",
    "Gross enrolment ratio for tertiary education, female (%), GER.5T8.F": "Gross enrollment ratio in tertiary education among women",
    "Gross enrolment ratio for tertiary education, male (%), GER.5T8.M": "Gross enrollment ratio in tertiary education among men",
    "Initial government funding per lower secondary student, constant PPP$, XUNIT.PPPCONST.2.FSGOV.FFNTR": "Government spending on lower secondary education per student",
    "Initial government funding per pre-primary student, constant PPP$, XUNIT.PPPCONST.02.FSGOV.FFNTR": "Government spending on pre-primary education per student",
    "Initial government funding per primary student, constant PPP$, XUNIT.PPPCONST.1.FSGOV.FFNTR": "Government spending on primary education per student",
    "Initial government funding per tertiary student, constant PPP$, XUNIT.PPPCONST.5T8.FSGOV.FFNTR": "Government spending on tertiary education per student",
    "Initial government funding per upper secondary student, constant PPP$, XUNIT.PPPCONST.3.FSGOV.FFNTR": "Government spending on upper secondary education per student",
    "Net enrolment rate, pre-primary, both sexes (%), NER.02.CP": "Net enrollment rate in pre-primary education",
    "Net enrolment rate, pre-primary, female (%), NER.02.F.CP": "Net enrollment rate in pre-primary education among girls",
    "Net enrolment rate, pre-primary, male (%), NER.02.M.CP": "Net enrollment rate in pre-primary education among boys",
    "Out-of-school rate for adolescents of lower secondary school age, adjusted gender parity index (GPIA) (modelled data), ROFST.MOD.2.GPIA": "Out-of-school rate for adolescents of lower secondary school age, adjusted gender parity index (GPIA), (modelled data), ROFST.MOD.2.GPIA",
    "Out-of-school rate for adolescents of lower secondary school age, both sexes (%), ROFST.2.CP": "Out-of-school rate for adolescents of lower secondary school age",
    "Out-of-school rate for adolescents of lower secondary school age, female (%), ROFST.2.F.CP": "Out-of-school rate for girls of lower secondary school age",
    "Out-of-school rate for adolescents of lower secondary school age, male (%), ROFST.2.M.CP": "Out-of-school rate for boys of lower secondary school age",
    "Out-of-school rate for children of primary school age, adjusted gender parity index (GPIA) (modelled data), ROFST.MOD.1.GPIA": "Out-of-school rate for children of primary school age, adjusted gender parity index (GPIA), (modelled data), ROFST.MOD.1.GPIA",
    "Out-of-school rate for children of primary school age, both sexes (%), ROFST.1.CP": "Out-of-school rate for children of primary school age",
    "Out-of-school rate for children of primary school age, female (%), ROFST.1.F.CP": "Out-of-school rate for girls of primary school age",
    "Out-of-school rate for children of primary school age, male (%), ROFST.1.M.CP": "Out-of-school rate for boys of primary school age",
    "Out-of-school rate for children one year before the official primary entry age, both sexes (%), ROFST.AGM1.CP": "Out-of-school rate for children one year before official primary entry age",
    "Out-of-school rate for children one year before the official primary entry age, female (%), ROFST.AGM1.F.CP": "Out-of-school rate for girls one year before official primary entry age",
    "Out-of-school rate for children one year before the official primary entry age, male (%), ROFST.AGM1.M.CP": "Out-of-school rate for boys one year before official primary entry age",
    "Out-of-school rate for youth of upper secondary school age, adjusted gender parity index (GPIA) (modelled data), ROFST.MOD.3.GPIA": "Out-of-school rate for youth of upper secondary school age, adjusted gender parity index (GPIA), (modelled data), ROFST.MOD.3.GPIA",
    "Out-of-school rate for youth of upper secondary school age, both sexes (%), ROFST.3.CP": "Out-of-school rate for youth of upper secondary school age",
    "Out-of-school rate for youth of upper secondary school age, female (%), ROFST.3.F.CP": "Out-of-school rate for girls of upper secondary school age",
    "Out-of-school rate for youth of upper secondary school age, male (%), ROFST.3.M.CP": "Out-of-school rate for boys of upper secondary school age",
    "Percentage of qualified teachers in lower secondary education, both sexes (%), QUTP.2": "Percentage of qualified teachers in lower secondary education",
    "Percentage of qualified teachers in pre-primary education, both sexes (%), QUTP.02": "Percentage of qualified teachers in pre-primary education",
    "Percentage of qualified teachers in primary education, both sexes (%), QUTP.1": "Percentage of qualified teachers in primary education",
    "Percentage of qualified teachers in secondary education, both sexes (%), QUTP.2T3": "Percentage of qualified teachers in secondary education",
    "Percentage of qualified teachers in upper secondary education, both sexes (%), QUTP.3": "Percentage of qualified teachers in upper secondary education",
    "Proportion of children/young people at the age of lower secondary education prepared for the future in mathematics, both sexes (%), PREPFUTURE.2.MATH": "Share of children achieving minimum math proficiency by the end of lower secondary age",
    "Proportion of children/young people at the age of lower secondary education prepared for the future in mathematics, female (%), PREPFUTURE.2.MATH.F": "Share of girls achieving minimum math proficiency by the end of lower secondary age",
    "Proportion of children/young people at the age of lower secondary education prepared for the future in mathematics, male (%), PREPFUTURE.2.MATH.M": "Share of boys achieving minimum math proficiency by the end of lower secondary age",
    "Proportion of children/young people at the age of lower secondary education prepared for the future in reading, both sexes (%), PREPFUTURE.2.READ": "Share of children achieving minimum reading proficiency by the end of lower secondary age",
    "Proportion of children/young people at the age of lower secondary education prepared for the future in reading, female (%), PREPFUTURE.2.READ.F": "Share of girls achieving minimum reading proficiency by the end of lower secondary age",
    "Proportion of children/young people at the age of lower secondary education prepared for the future in reading, male (%), PREPFUTURE.2.READ.M": "Share of boys achieving minimum reading proficiency by the end of lower secondary age",
    "Proportion of children/young people at the age of primary education prepared for the future in mathematics, both sexes (%), PREPFUTURE.1.MATH": "Share of children achieving minimum math proficiency by the end of primary age",
    "Proportion of children/young people at the age of primary education prepared for the future in mathematics, female (%), PREPFUTURE.1.MATH.F": "Share of girls achieving minimum math proficiency by the end of primary age",
    "Proportion of children/young people at the age of primary education prepared for the future in mathematics, male (%), PREPFUTURE.1.MATH.M": "Share of boys achieving minimum math proficiency by the end of primary age",
    "Proportion of children/young people at the age of primary education prepared for the future in reading, both sexes (%), PREPFUTURE.1.READ": "Share of children achieving minimum reading proficiency by the end of primary age",
    "Proportion of children/young people at the age of primary education prepared for the future in reading, female (%), PREPFUTURE.1.READ.F": "Share of girls achieving minimum reading proficiency by the end of primary age",
    "Proportion of children/young people at the age of primary education prepared for the future in reading, male (%), PREPFUTURE.1.READ.M": "Share of boys achieving minimum reading proficiency by the end of primary age",
    "Proportion of primary schools with access to basic drinking water (%), SCHBSP.1.WWATA": "Share of primary schools with access to drinking water",
    "Proportion of primary schools with single-sex basic sanitation facilities (%), SCHBSP.1.WTOILA": "Share of primary schools with single-sex sanitation facilities",
    "Proportion of students at the end of lower secondary education achieving at least a minimum proficiency level in mathematics, both sexes (%), MATH.LOWERSEC": "Share of students at the end of lower secondary education with minimum math skills",
    "Proportion of students at the end of lower secondary education achieving at least a minimum proficiency level in mathematics, female (%), MATH.LOWERSEC.F": "Share of female students at the end of lower secondary education with minimum math skills",
    "Proportion of students at the end of lower secondary education achieving at least a minimum proficiency level in mathematics, male (%), MATH.LOWERSEC.M": "Share of male students at the end of lower secondary education with minimum math skills",
    "Proportion of students at the end of lower secondary education achieving at least a minimum proficiency level in reading, both sexes (%), READ.LOWERSEC": "Share of students at the end of lower secondary education with minimum reading skills",
    "Proportion of students at the end of lower secondary education achieving at least a minimum proficiency level in reading, female (%), READ.LOWERSEC.F": "Share of female students at the end of lower secondary education with minimum reading skills",
    "Proportion of students at the end of lower secondary education achieving at least a minimum proficiency level in reading, male (%), READ.LOWERSEC.M": "Share of male students at the end of lower secondary education with minimum reading skills",
    "Proportion of students at the end of primary education achieving at least a minimum proficiency level in mathematics, both sexes (%), MATH.PRIMARY": "Share of students with minimum math skills by the end of primary school",
    "Proportion of students at the end of primary education achieving at least a minimum proficiency level in mathematics, female (%), MATH.PRIMARY.F": "Share of female students with minimum math skills by the end of primary school",
    "Proportion of students at the end of primary education achieving at least a minimum proficiency level in mathematics, male (%), MATH.PRIMARY.M": "Share of male students with minimum math skills by the end of primary school",
    "Proportion of students at the end of primary education achieving at least a minimum proficiency level in reading, both sexes (%), READ.PRIMARY": "Share of students with minimum reading skills by the end of primary school",
    "Proportion of students at the end of primary education achieving at least a minimum proficiency level in reading, female (%), READ.PRIMARY.F": "Share of female students with minimum reading skills by the end of primary school",
    "Proportion of students at the end of primary education achieving at least a minimum proficiency level in reading, male (%), READ.PRIMARY.M": "Share of male students with minimum reading skills by the end of primary school",
    "Pupil-qualified teacher ratio in pre-primary education (headcount basis), PTRHC.02.QUALIFIED": "Pupil-qualified teacher ratio in pre-primary education",
    "Pupil-qualified teacher ratio in primary education (headcount basis), PTRHC.1.QUALIFIED": "Pupil-qualified teacher ratio in primary education",
    "Youth literacy rate, population 15-24 years, adjusted gender parity index (GPIA), LR.AG15T24.GPIA": "Literacy rate among young people (15\u201324 years), adjusted gender parity index",
    "Youth literacy rate, population 15-24 years, both sexes (%), LR.AG15T24": "Literacy rate among young people (15\u201324 years)",
    "Youth literacy rate, population 15-24 years, female (%), LR.AG15T24.F": "Literacy rate among young women",
    "Youth literacy rate, population 15-24 years, male (%), LR.AG15T24.M": "Literacy rate among young men",
}

# Unit overrides for indicators whose units need specific values not inferrable from the label.
_UNIT_OVERRIDES = {
    "Pupil-qualified teacher ratio in pre-primary education (headcount basis), PTRHC.02.QUALIFIED": (
        1,
        "pupils per teacher",
        " ",
    ),
    "Pupil-qualified teacher ratio in primary education (headcount basis), PTRHC.1.QUALIFIED": (
        1,
        "pupils per teacher",
        " ",
    ),
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
        # Keep one row per group, preferring non-null values
        tb_to_consolidate = tb_to_consolidate.sort_values("value", na_position="last")
        tb_to_consolidate = tb_to_consolidate.drop_duplicates(subset=["year", "indicator_id"], keep="first")
        if hasattr(tb_to_consolidate["country"].dtype, "categories"):
            tb_to_consolidate["country"] = tb_to_consolidate["country"].astype(str)
        tb_to_consolidate["country"] = "World"

    parts = [p for p in [tb_other, tb_keep_as_is, tb_to_consolidate] if not p.empty]
    return pr.concat(parts) if len(parts) > 1 else parts[0]


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("education_sdgs")
    ds_expenditure = paths.load_dataset("public_expenditure")

    # Read table from meadow dataset.
    tb = ds_meadow.read("education_sdgs")

    # Load historical expenditure data
    tb_expenditure = ds_expenditure.read("public_expenditure")

    # Retrieve snapshot with the metadata provided via World Bank.
    snap_wb = paths.load_snapshot("edstats_metadata.xls")
    tb_wb = snap_wb.read()

    #
    # Process data.
    #
    country_mapping_path = paths.directory / "education.countries.json"
    excluded_countries_path = paths.directory / "education.excluded_countries.json"
    tb = paths.regions.harmonize_names(
        tb, country_col="country", countries_file=country_mapping_path, excluded_countries_file=excluded_countries_path
    )
    tb = consolidate_world_entries(tb)
    # Drop columns that are not needed
    tb = tb.drop(columns=["magnitude", "qualifier"])

    # Build long-description lookup from World Bank metadata (keyed by indicator label)
    long_definition_map = {}
    for indicator in tb["indicator_label_en"].unique():
        defn = tb_wb[tb_wb["Indicator Name"] == indicator]["Long definition"].values
        long_definition_map[indicator] = defn[0] if len(defn) > 0 else ""

    tb["long_description"] = tb["indicator_label_en"].map(long_definition_map)

    # Drop rows with missing indicator labels
    tb = tb[tb["indicator_label_en"].notna()]
    tb["indicator_label_en"] = tb["indicator_label_en"].astype(str) + ", " + tb["indicator_id"].astype(str)

    # Pivot the table to have indicators as columns
    tb_pivoted = tb.pivot(index=["country", "year"], columns="indicator_label_en", values="value")

    # Assign metadata for every column
    long_desc_lookup = tb.set_index("indicator_label_en")["long_description"]
    for column in tb_pivoted.columns:
        meta = tb_pivoted[column].metadata
        meta.display = {}
        # Apply curated title where available, otherwise use the raw indicator label
        meta.title = _TITLE_OVERRIDES.get(column, column)
        if column in long_desc_lookup.index:
            desc = long_desc_lookup[column]
            meta.description_from_producer = desc.iloc[0] if hasattr(desc, "iloc") else desc
        decimals, unit, short_unit = _unit_info(column)
        if column in _UNIT_OVERRIDES:
            decimals, unit, short_unit = _UNIT_OVERRIDES[column]
        update_metadata(meta, display_decimals=decimals, unit=unit, short_unit=short_unit)

    tb_pivoted = tb_pivoted.reset_index()

    # Remove Turkey 1998 value for Government expenditure on education as a percentage of GDP (%), XGDP.FSGOV (likely an error)
    mask = (tb_pivoted["country"] == "Turkey") & (tb_pivoted["year"] == 1998)
    tb_pivoted.loc[mask, "Government expenditure on education as a percentage of GDP (%), XGDP.FSGOV"] = None

    tb_pivoted = tb_pivoted.format(["country", "year"])

    # Rename columns that UNESCO relabelled between releases, to preserve backward compatibility
    # with existing charts and MDim steps that reference the old short_names.
    existing_renames = {k: v for k, v in _SHORT_NAME_RENAMES.items() if k in tb_pivoted.columns}
    if existing_renames:
        tb_pivoted = tb_pivoted.rename(columns=existing_renames)

    # Combine recent literacy estimates and expenditure data with historical estimates from a migrated dataset
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


def _unit_info(column: str) -> tuple:
    """Return (display_decimals, unit, short_unit) from an indicator label.

    Covers all unit suffixes observed in the UNESCO SDG dataset.
    """
    col = column.lower()
    if "%" in col:
        return 1, "%", "%"
    elif "(days)" in col:
        return 1, "days", ""
    elif "(years)" in col:
        return 1, "years", ""
    elif any(pia in col for pia in ("gpia", "lpia", "wpia", "npia", "dpia", "ltpia")):
        # Gender/learning/wealth/etc. parity index — dimensionless ratio
        return 2, "index", ""
    elif "index" in col:
        return 1, "index", ""
    elif "(current us$)" in col:
        return 0, "current US$", "$"
    elif "ppp$" in col:
        return 0, "constant 2019 US$", "$"
    elif "us$" in col or " usd" in col:
        return 0, "current US$", "$"
    elif "(number)" in col:
        return 0, "number", ""
    else:
        return 0, " ", " "


def update_metadata(meta: VariableMeta, display_decimals: int, unit: str, short_unit: str) -> None:
    """Update metadata unit attributes in-place."""
    meta.display["numDecimalPlaces"] = display_decimals
    meta.unit = unit
    meta.short_unit = short_unit
