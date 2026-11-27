"""Load the Women, Business and the Law (WBL 1.0) snapshots and create a meadow dataset.

The World Bank's Gender Statistics stopped publishing the WBL 1.0 indicators (yes/no legal questions and scores) when
WBL moved to its 2.0 framework. To keep those series going, we take them directly from WBL: the 1971-2024 historical
panel, and the 2026 report recalculated under the 1.0 methodology.

The 2026 file's "Data Changes" sheet lists every answer that changed since the 2024 report, either a reform (a change in
the law) or a revision (a correction of an earlier answer). The World Bank never corrected the panel for the revisions,
but shared, for each change, the first report year it applies to. We keep that sheet as a second table, and the garden
step applies the revisions to the panel.

NOTE: The World Bank plans to publish a new panel under the WBL 2.0 methodology in March 2027, which replaces both files.

Columns are renamed to the World Bank indicator codes that Gender Statistics used for the same series, so that the
garden step can slot them back into the gender_statistics table.
"""

import re

from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Sheet holding the full data in each snapshot.
SHEETS = {
    "women_business_and_the_law_1_0_panel.xlsx": "WBL Panel 2024",
    "women_business_and_the_law_1_0_scores.xlsx": "WBL 2026 1.0",
}
# Snapshot and sheet listing the changes since the 2024 report, and their new names.
CHANGES_SNAPSHOT = "women_business_and_the_law_1_0_scores.xlsx"
CHANGES_SHEET = "Data Changes"
CHANGES_COLUMNS = {
    "Economy": "country",
    "Question": "indicator",
    "Category": "category",
    "Original Value": "original_value",
    "New Value": "new_value",
    "Back-calculate to WBL Report Year": "first_report_year",
}

# Identifier columns we keep, and their new names (region and income group are used for the World Bank's averages).
ID_COLUMNS = {"Economy": "country", "Report Year": "report_year", "Region": "region", "Income Group": "income_group"}
# Identifier columns we drop.
DROP_COLUMNS = ["Economy Code", "ISO Code"]

# Map each (normalized) column header to the World Bank indicator code that Gender Statistics used for it.
HEADER_TO_CODE = {
    "wbl index": "sg_law_indx",
    # Mobility.
    "mobility": "sg_law_indx_mo",
    "can a woman choose where to live in the same way as a man?": "sg_loc_live_eq",
    "can a woman travel outside her home in the same way as a man?": "sg_hme_trvl_eq",
    "can a woman apply for a passport in the same way as a man?": "sg_apl_pspt_eq",
    "can a woman travel outside the country in the same way as a man?": "sg_ctr_trvl_eq",
    # Workplace.
    "workplace": "sg_law_indx_wp",
    "can a woman get a job in the same way as a man?": "sg_get_jobs_eq",
    "does the law prohibit discrimination in employment based on gender?": "sg_law_nodc_hr",
    "is there legislation on sexual harassment in employment?": "sg_leg_sxhr_em",
    "are there criminal penalties or civil remedies for sexual harassment in employment?": "sg_pen_sxhr_em",
    # Pay.
    "pay": "sg_law_indx_py",
    "does the law mandate equal remuneration for work of equal value?": "sg_law_eqrm_wk",
    "can a woman work at night in the same way as a man?": "sg_ngt_work_eq",
    "can a woman work in a job deemed dangerous in the same way as a man?": "sg_dng_work_dn_eq",
    "can a woman work in an industrial job in the same way as a man?": "sg_ind_work_eq",
    # Marriage.
    "marriage": "sg_law_indx_mr",
    "is the law free of legal provisions that require a married woman to obey her husband?": "sg_law_obhb_mr_no",
    "can a woman be head of household in the same way as a man?": "sg_hld_head_eq",
    "is there legislation specifically addressing domestic violence?": "sg_leg_dvaw",
    "can a woman obtain a judgment of divorce in the same way as a man?": "sg_obt_dvrc_eq",
    "does a woman have the same rights to remarry as a man?": "sg_rem_rigt_eq",
    # Parenthood.
    "parenthood": "sg_law_indx_pr",
    "is paid leave of at least 14 weeks available to mothers?": "sh_mmr_leve_al",
    "length of paid maternity leave": "sh_mmr_leve",
    "does the government administer 100% of maternity leave benefits?": "sh_mmr_leve_gt",
    "is there paid leave available to fathers?": "sh_ptr_leve_al",
    "length of paid paternity leave": "sh_ptr_leve",
    "is there paid parental leave?": "sh_par_leve_al",
    "shared days": "sh_par_leve",
    "days for the mother": "sh_par_leve_fe",
    "days for the father": "sh_par_leve_ma",
    "is dismissal of pregnant workers prohibited?": "sg_dml_prgw",
    # Entrepreneurship.
    "entrepreneurship": "sg_law_indx_en",
    "does the law prohibit discrimination in access to credit based on gender?": "sg_law_crdd_gr",
    "can a woman sign a contract in the same way as a man?": "sg_cnt_sign_eq",
    "can a woman register a business in the same way as a man?": "sg_bus_regt_eq",
    "can a woman open a bank account in the same way as a man?": "sg_opn_bank_eq",
    # Assets.
    "assets": "sg_law_indx_as",
    "do women and men have equal ownership rights to immovable property?": "sg_own_prrt_im",
    "do sons and daughters have equal rights to inherit assets from their parents?": "sg_iht_asst_pt_eq",
    "do male and female surviving spouses have equal rights to inherit assets?": "sg_iht_asst_eq",
    "does the law grant spouses equal administrative authority over assets during marriage?": "sg_law_asst_ar",
    "does the law provide for the valuation of nonmonetary contributions?": "sg_law_nmcn",
    # Pension.
    "pension": "sg_law_indx_pe",
    "is the age at which women and men can retire with full pension benefits the same?": "sg_age_fupn_eq",
    "is the age at which women and men can retire with partial pension benefits the same?": "sg_age_papn_eq",
    "is the mandatory retirement age for women and men the same?": "sg_age_mret_eq",
    "are periods of absence due to childcare accounted for in pension benefits?": "sg_abs_penb",
}


def run() -> None:
    #
    # Load inputs.
    #
    tables = []
    for snapshot_name, sheet_name in SHEETS.items():
        snap = paths.load_snapshot(snapshot_name)
        tb = snap.read_excel(sheet_name=sheet_name)

        # Rename columns to indicator codes, failing on any header we don't recognize.
        tb = tb.drop(columns=DROP_COLUMNS)
        renames = {column: HEADER_TO_CODE.get(normalize_header(column)) for column in tb.columns}
        renames.update(ID_COLUMNS)
        unknown = sorted(column for column, code in renames.items() if code is None)
        assert not unknown, f"Unknown column headers in {snapshot_name}: {unknown}"
        tb = tb.rename(columns=renames, errors="raise")
        assert set(tb.columns) == set(HEADER_TO_CODE.values()) | set(ID_COLUMNS.values()), (
            f"Unexpected set of columns in {snapshot_name}."
        )
        tables.append(tb)

    # Load the list of changes since the 2024 report.
    tb_changes = paths.load_snapshot(CHANGES_SNAPSHOT).read_excel(sheet_name=CHANGES_SHEET)

    #
    # Process data.
    #
    # Combine the tables from all snapshots.
    tb = pr.concat(tables, ignore_index=True)
    for column in ["country", "region", "income_group"]:
        tb[column] = tb[column].astype("category")

    # Name each change's question by its indicator code, and its first report year as a number (e.g. "WBL2022" -> 2022).
    tb_changes = tb_changes[list(CHANGES_COLUMNS)].rename(columns=CHANGES_COLUMNS, errors="raise")
    tb_changes["indicator"] = tb_changes["indicator"].map(
        lambda question: HEADER_TO_CODE.get(normalize_header(question))
    )
    assert tb_changes["indicator"].notna().all(), "Unknown question in the list of changes."
    first_year = tb_changes["first_report_year"].astype(str).str.fullmatch(r"WBL(\d{4})")
    assert first_year.all(), "Unexpected format of the first report year of a change."
    tb_changes["first_report_year"] = tb_changes["first_report_year"].astype(str).str[3:].astype(int)
    for column in ["country", "indicator", "category"]:
        tb_changes[column] = tb_changes[column].astype("category")

    # Improve table format.
    tb = tb.format(["country", "report_year"], short_name=paths.short_name)
    tb_changes = tb_changes.format(["country", "indicator"], short_name=f"{paths.short_name}_changes")

    #
    # Save outputs.
    #
    ds_meadow = paths.create_dataset(tables=[tb, tb_changes])
    ds_meadow.save()


def normalize_header(header: str) -> str:
    """Normalize a column header, so that small wording differences between WBL files map to the same code."""
    header = re.sub(r"\s+", " ", str(header)).strip().lower()
    header = header.replace("wbl 1.0 index", "wbl index")
    header = header.replace("100 percent", "100%")
    header = header.replace("men and women", "women and men")
    return header
