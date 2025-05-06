"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


COLUMNS_KEEP = [
    # primary keys
    "year",
    "country_name",
    # e
    "e_ti_cpi",
    "e_wbgi_gee",
    # v2ca
    "v2cademmob",
    "v2cademmob_codehigh",
    "v2cademmob_codelow",
    "v2caautmob",
    "v2caautmob_codehigh",
    "v2caautmob_codelow",
    "v2cacamps",
    "v2cacamps_codehigh",
    "v2cacamps_codelow",
    "v2caviol",
    "v2caviol_codehigh",
    "v2caviol_codelow",
    # v2cl
    "v2clacjstm_osp",
    "v2clacjstm_osp_codehigh",
    "v2clacjstm_osp_codelow",
    "v2clacjstw_osp",
    "v2clacjstw_osp_codehigh",
    "v2clacjstw_osp_codelow",
    "v2clrspct",
    "v2clrspct_codehigh",
    "v2clrspct_codelow",
    "v2clsocgrp",
    "v2clsocgrp_codehigh",
    "v2clsocgrp_codelow",
    "v2cltrnslw_osp",
    "v2cltrnslw_osp_codehigh",
    "v2cltrnslw_osp_codelow",
    # v2d
    "v2dlcommon",
    "v2dlcommon_codehigh",
    "v2dlcommon_codelow",
    "v2dlconslt",
    "v2dlconslt_codehigh",
    "v2dlconslt_codelow",
    "v2dlcountr",
    "v2dlcountr_codehigh",
    "v2dlcountr_codelow",
    "v2dlengage",
    "v2dlengage_codehigh",
    "v2dlengage_codelow",
    "v2dlreason",
    "v2dlreason_codehigh",
    "v2dlreason_codelow",
    # v2e
    "v2elfrfair_osp",
    "v2elfrfair_osp_codehigh",
    "v2elfrfair_osp_codelow",
    "v2elmulpar_osp",
    "v2elmulpar_osp_codehigh",
    "v2elmulpar_osp_codelow",
    "v2eltrnout",
    "v2elvaptrn",
    # What type of election was held on this date?
    "v2eltype_0",  # Legislative; lower, sole, or both chambers, first or only round.
    "v2eltype_1",  # Legislative; lower, sole, or both chambers, second or later round.
    "v2eltype_2",  # Legislative; upper chamber only, first or only round.
    "v2eltype_3",  # Legislative; upper chamber only, second round.
    "v2eltype_4",  # Constituent Assembly, first or only round.
    "v2eltype_5",  # Constituent Assembly, second or later round.
    "v2eltype_6",  # Presidential, first or only round.
    "v2eltype_7",  # Presidential, second round.
    "v2eltype_8",  # Metropolitan or supranational legislative, first or only round.
    "v2eltype_9",  # Metropolitan or supranational legislative, second round.
    "v2ex_hogw",  # HOG have more relative power than the HOS over the appointment and dismissal of cabinet ministers?
    "v2ex_hosw",  # HOS have more relative power than the HOG over the appointment and dismissal of cabinet ministers?
    "v2ex_legconhog",
    "v2ex_legconhos",
    "v2exaphogp",
    "v2exfemhog",
    "v2exfemhos",
    "v2exhoshog",  # Is the head of state (HOS) also head of government (HOG)?
    "v2exnamhog",  # What is the name of the head of government?
    "v2exnamhos",  # What is the name of the head of state?
    "v2expathhg",
    "v2expathhs",
    # v2j
    "v2jucorrdc",
    "v2jucorrdc_codehigh",
    "v2jucorrdc_codelow",
    # v2l
    "v2lgcrrpt",
    "v2lgcrrpt_codehigh",
    "v2lgcrrpt_codelow",
    "v2lgfemleg",
    # v2m
    "v2mecenefm",
    "v2mecenefm_codehigh",
    "v2mecenefm_codelow",
    "v2meharjrn",
    "v2meharjrn_codehigh",
    "v2meharjrn_codelow",
    "v2meslfcen",
    "v2meslfcen_codehigh",
    "v2meslfcen_codelow",
    # v2p
    "v2pepwrsoc",
    "v2pepwrsoc_codehigh",
    "v2pepwrsoc_codelow",
    # v2s
    "v2svdomaut",
    "v2svdomaut_codehigh",
    "v2svdomaut_codelow",
    "v2svinlaut",
    "v2svinlaut_codehigh",
    "v2svinlaut_codelow",
    "v2svstterr",
    "v2svstterr_codehigh",
    "v2svstterr_codelow",
    # v2x
    "v2x_civlib",
    "v2x_civlib_codehigh",
    "v2x_civlib_codelow",
    "v2x_clphy",
    "v2x_clphy_codehigh",
    "v2x_clphy_codelow",
    "v2x_clpol",
    "v2x_clpol_codehigh",
    "v2x_clpol_codelow",
    "v2x_clpriv",
    "v2x_clpriv_codehigh",
    "v2x_clpriv_codelow",
    "v2x_corr",
    "v2x_corr_codehigh",
    "v2x_corr_codelow",
    "v2x_cspart",
    "v2x_cspart_codehigh",
    "v2x_cspart_codelow",
    "v2x_delibdem",
    "v2x_delibdem_codehigh",
    "v2x_delibdem_codelow",
    "v2x_egal",
    "v2x_egal_codehigh",
    "v2x_egal_codelow",
    "v2x_egaldem",
    "v2x_egaldem_codehigh",
    "v2x_egaldem_codelow",
    "v2x_elecoff",
    "v2x_elecreg",
    "v2x_elecreg",
    "v2x_execorr",
    "v2x_execorr_codehigh",
    "v2x_execorr_codelow",
    "v2x_frassoc_thick",
    "v2x_frassoc_thick_codehigh",
    "v2x_frassoc_thick_codelow",
    "v2x_freexp_altinf",
    "v2x_freexp_altinf_codehigh",
    "v2x_freexp_altinf_codelow",
    "v2x_gencl",
    "v2x_gencl_codehigh",
    "v2x_gencl_codelow",
    "v2x_gencs",
    "v2x_gencs_codehigh",
    "v2x_gencs_codelow",
    "v2x_gender",
    "v2x_gender_codehigh",
    "v2x_gender_codelow",
    "v2x_genpp",
    "v2x_genpp_codehigh",
    "v2x_genpp_codelow",
    "v2x_jucon",
    "v2x_jucon_codehigh",
    "v2x_jucon_codelow",
    "v2x_libdem",
    "v2x_libdem_codehigh",
    "v2x_libdem_codelow",
    "v2x_liberal",
    "v2x_liberal_codehigh",
    "v2x_liberal_codelow",
    "v2x_partip",
    "v2x_partip_codehigh",
    "v2x_partip_codelow",
    "v2x_partipdem",
    "v2x_partipdem_codehigh",
    "v2x_partipdem_codelow",
    "v2x_polyarchy",
    "v2x_polyarchy_codehigh",
    "v2x_polyarchy_codelow",
    "v2xpas_democracy",
    "v2xpas_democracy_government",
    "v2xpas_democracy_opposition",
    "v2x_pubcorr",
    "v2x_pubcorr_codehigh",
    "v2x_pubcorr_codelow",
    "v2x_regime",
    "v2x_regime_amb",
    "v2x_rule",
    "v2x_rule_codehigh",
    "v2x_rule_codelow",
    "v2x_suffr",
    "v2xca_academ",
    "v2xca_academ_codehigh",
    "v2xca_academ_codelow",
    "v2xcl_rol",
    "v2xcl_rol_codehigh",
    "v2xcl_rol_codelow",
    "v2xcs_ccsi",
    "v2xcs_ccsi_codehigh",
    "v2xcs_ccsi_codelow",
    "v2xdd_dd",
    "v2xdl_delib",
    "v2xdl_delib_codehigh",
    "v2xdl_delib_codelow",
    "v2xeg_eqaccess",
    "v2xeg_eqaccess_codehigh",
    "v2xeg_eqaccess_codelow",
    "v2xeg_eqdr",
    "v2xeg_eqdr_codehigh",
    "v2xeg_eqdr_codelow",
    "v2xeg_eqprotec",
    "v2xeg_eqprotec_codehigh",
    "v2xeg_eqprotec_codelow",
    "v2xel_frefair",
    "v2xel_frefair_codehigh",
    "v2xel_frefair_codelow",
    "v2xel_locelec",
    "v2xel_locelec_codehigh",
    "v2xel_locelec_codelow",
    "v2xel_regelec",
    "v2xel_regelec_codehigh",
    "v2xel_regelec_codelow",
    "v2xex_elecreg",
    "v2xex_elecreg",
    "v2xlg_elecreg",
    "v2xlg_elecreg",
    "v2xlg_legcon",
    "v2xlg_legcon_codehigh",
    "v2xlg_legcon_codelow",
    "v2xnp_pres",
    "v2xnp_pres_codehigh",
    "v2xnp_pres_codelow",
    # Historical name
    "histname",
]


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("vdem.zip")

    # Load data from snapshot.
    with snap.open_archive():
        tb = snap.read_from_archive("V-Dem-CY-Full+Others-v15.csv", usecols=COLUMNS_KEEP, dtype={"v2exnamhog": "str"})
    #
    # Process data.
    #
    # Column rename
    tb = tb.rename(
        columns={
            "country_name": "country",
        }
    )
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
