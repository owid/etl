"""Load a garden dataset and create a grapher dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Women, Business and the Law (WBL 1.0) yes/no questions, stored as 1/0 in garden (see the garden step's WBL_TOPICS).
# Garden keeps 1/0 because the country counts step sums them; here they become "No"/"Yes", so the Swimlane chart can
# show them.
# NOTE: On each update, check this list against WBL_TOPICS in the garden step, and that the answers are still 1/0.
WBL_QUESTIONS = [
    "sg_loc_live_eq",
    "sg_hme_trvl_eq",
    "sg_apl_pspt_eq",
    "sg_ctr_trvl_eq",
    "sg_get_jobs_eq",
    "sg_law_nodc_hr",
    "sg_leg_sxhr_em",
    "sg_pen_sxhr_em",
    "sg_law_eqrm_wk",
    "sg_ngt_work_eq",
    "sg_dng_work_dn_eq",
    "sg_ind_work_eq",
    "sg_law_obhb_mr_no",
    "sg_hld_head_eq",
    "sg_leg_dvaw",
    "sg_obt_dvrc_eq",
    "sg_rem_rigt_eq",
    "sh_mmr_leve_al",
    "sh_mmr_leve_gt",
    "sh_ptr_leve_al",
    "sh_par_leve_al",
    "sg_dml_prgw",
    "sg_law_crdd_gr",
    "sg_cnt_sign_eq",
    "sg_bus_regt_eq",
    "sg_opn_bank_eq",
    "sg_own_prrt_im",
    "sg_iht_asst_pt_eq",
    "sg_iht_asst_eq",
    "sg_law_asst_ar",
    "sg_law_nmcn",
    "sg_age_fupn_eq",
    "sg_age_papn_eq",
    "sg_age_mret_eq",
    "sg_abs_penb",
]
ANSWER_LABELS = {0: "No", 1: "Yes"}


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("gender_statistics")

    # Read table from garden dataset.
    tb = ds_garden.read("gender_statistics", reset_index=False)

    #
    # Process data.
    #
    # Store the WBL yes/no answers as text.
    tb = answers_to_no_yes(tb, WBL_QUESTIONS)

    #
    # Save outputs.
    #
    # Initialize a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata)

    # Save grapher dataset.
    ds_grapher.save()


def answers_to_no_yes(tb: Table, columns: list[str]) -> Table:
    """Replace the 1/0 answers of WBL questions with "Yes"/"No", as categorical indicators.

    The unit "(1=yes; 0=no)" no longer applies, so it is cleared.
    """
    missing = set(columns) - set(tb.columns)
    assert not missing, f"WBL questions missing from the garden table: {sorted(missing)}"
    for column in columns:
        unknown = set(tb[column].dropna().unique()) - set(ANSWER_LABELS)
        assert not unknown, f"Column `{column}` has values other than 0 and 1: {unknown}"
        metadata = tb[column].metadata
        tb[column] = tb[column].map(ANSWER_LABELS).astype("category")
        tb[column].metadata = metadata
        tb[column].metadata.type = "string"
        tb[column].metadata.sort = list(ANSWER_LABELS.values())
        tb[column].metadata.unit = ""
        tb[column].metadata.short_unit = ""
    return tb
