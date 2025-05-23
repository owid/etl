"""Load a meadow dataset and create a garden dataset."""

from typing import cast

import vdem_aggregate as aggregate  # VDEM's aggregating library
import vdem_clean as clean  # VDEM's cleaning library
import vdem_impute as impute  # VDEM's imputing library
import vdem_refine as refine  # VDEM's imputing library
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# REGION AGGREGATES
REGIONS = {
    "Africa": {
        "additional_members": [
            "Somaliland",
            "Zanzibar",
        ]
    },
    "Asia": {
        "additional_members": [
            "Palestine/Gaza",
            "Palestine/West Bank",
        ]
    },
    "North America": {},
    "South America": {},
    "Europe": {
        "additional_members": [
            "Brunswick",
            "Duchy of Nassau",
            "Hamburg",
            "Hanover",
            "Hesse Electoral",
            "Hesse Grand Ducal",
            "Mecklenburg Schwerin",
            "Modena",
            "Oldenburg",
            "Piedmont-Sardinia",
            "Saxe-Weimar-Eisenach",
            "Saxony",
            "Tuscany",
            "Two Sicilies",
            "Wurttemberg",
        ]
    },
    "Oceania": {},
}
# INDICATORS THAT FOR SOME UNKNOWN REASON HAVE LOST THEIR ORIGINS DURING THE PROCESSING
INDICATORS_NO_ORIGINS = [
    "regime_row_owid",
    "regime_amb_row_owid",
    "regime_redux_row_owid",
    "wom_hoe_vdem",
    "regime_imputed",
    "num_years_in_electdem_consecutive",
    "num_years_in_libdem_consecutive",
    "num_years_in_electdem",
    "num_years_in_libdem",
    "num_years_in_electdem_consecutive_cat",
    "num_years_in_libdem_consecutive_cat",
]


def run(dest_dir: str) -> None:
    # %% Load data
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("vdem")
    ds_regions = paths.load_dataset("regions")
    ds_population = paths.load_dataset("population")

    # Read table from meadow dataset.
    tb = ds_meadow["vdem"].reset_index()
    tb = cast(Table, tb.astype({"v2exnamhos": str}))

    #
    # Process data.
    #
    # %% Copy origins
    # Copy origins (some indicators will loose their 'origins' in metadata)
    origins = tb["country"].metadata.origins

    # %% PART 1: CLEAN
    # The following lines (until "PART 2") are the cleaning steps.
    # This is a transcription from Bastian's work: https://github.com/owid/notebooks/blob/main/BastianHerre/democracy/scripts/vdem_row_clean.do
    paths.log.info("1/ Cleaning data...")
    tb = clean.run(tb, paths.country_mapping_path)

    # %% PART 2: IMPUTE
    # The following lines concern imputing steps.
    # Equivalent to: https://github.com/owid/notebooks/blob/main/BastianHerre/democracy/scripts/vdem_row_impute.do
    paths.log.info("2/ Imputing data...")
    tb = impute.run(tb)

    # %% PART 3: REFINE
    paths.log.info("3/ Refining data...")
    tb = refine.run(tb)

    # %% PART 4: AGGREGATES
    paths.log.info("4/ Aggregating data...")
    tb_uni, tb_multi_without_regions, tb_multi_with_regions, tb_countries_counts, tb_population_counts = aggregate.run(
        tb, ds_regions, ds_population
    )

    # %% PART 5: Format and prepare tables
    paths.log.info("5/ Formatting tables...")
    tb_uni = tb_uni.format()
    tb_multi_without_regions = tb_multi_without_regions.format(
        keys=["country", "year", "estimate"], short_name="vdem_multi_without_regions"
    )
    tb_multi_with_regions = tb_multi_with_regions.format(
        keys=["country", "year", "estimate", "aggregate_method"], short_name="vdem_multi_with_regions"
    )
    tb_countries_counts = tb_countries_counts.format(
        keys=["country", "year", "category"], short_name="vdem_num_countries"
    )
    tb_population_counts = tb_population_counts.format(
        keys=["year", "country", "category"], short_name="vdem_population"
    )

    tables = [
        # Main indicators (uni-dimensional)
        tb_uni,  # some have 0 origins
        # Main indicators (multi-dimensional) without regions
        tb_multi_without_regions,
        # Main indicators (multi-dimensional) with regions
        tb_multi_with_regions,
        # Number of countries with X properties
        tb_countries_counts,
        # Number of people living in countries with X property
        tb_population_counts,
    ]

    # %% Add origins in case any was lost, adjust citation full
    for tb in tables:
        for col in tb.columns:
            if col in INDICATORS_NO_ORIGINS:
                assert (
                    len(tb[col].metadata.origins) == 0
                ), f"No origins expected for indicator {col} in table {tb.m.short_name}"
                tb[col].metadata.origins = origins
            if len(tb[col].metadata.origins) == 0:
                raise ValueError(f"No source for indicator {col} in table {tb.m.short_name}")

    # %% Save
    #
    # Save outputs.
    #
    # Tweak citation full for some indicators
    tb = adjust_citation_full(tb.copy())

    # %% Set index
    # tb = tb.format()

    # %% Save
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=tables, check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


# %%
def adjust_citation_full(tb: Table) -> Table:
    """Adjust the citation_full metadata field for some indicators."""
    tb = replace_citation_full(tb)
    tb = append_citation_full(tb)
    return tb


def replace_citation_full(tb: Table) -> Table:
    """Replace the citation_full metadata field for some indicators."""
    return tb


def append_citation_full(tb: Table) -> Table:
    """Add additional citations.

    Some indicators require additional citation information.
    """
    CITATION_COPPEDGE = "Coppedge et al. (2015, V-Dem Working Paper Series 2015:6)"
    CITATION_PEMSTEIN = "Pemstein et al. (2024, V-Dem Working Paper Series 2024:21)"
    CITATION_SIGMAN = "Sigman et al. (2015, V-Dem Working Paper Series 2015:22)"
    CITATION_MCMANN = "McMann et al. (2016, V-Dem Working Paper Series 2016:23)"
    CITATION_SUNDSTROM = "Sundström et al. (2017, V-Dem Working Paper Series 2017:19)"
    CITATION_TEORELL = "Teorell et al. (2019)"
    CITATION_LUHRMANN = "Lührmann, Anna, Marcus Tannnberg, and Staffan Lindberg. 2018. Regimes of the World (RoW): Opening New Avenues for the Comparative Study of Political Regimes. Politics and Governance 6(1): 60-77."

    citation_coppedge = [
        "libdem_vdem",
        "participdem_vdem",
        "delibdem_vdem",
        "lib_vdem",
        "particip_vdem",
        "delib_vdem",
        # row indicators
        "lib_dich_row",
    ]
    citation_pemstein = [
        "freeexpr_vdem",
        "freeassoc_vdem",
        "electfreefair_vdem",
        "indiv_libs_vdem",
        "judicial_constr_vdem",
        "legis_constr_vdem",
        "civsoc_particip_vdem",
        "corr_leg_vdem",
        "justified_polch_vdem",
        "justcomgd_polch_vdem",
        "counterarg_polch_vdem",
        "elitecons_polch_vdem",
        "soccons_polch_vdem",
        "corr_jud_vdem",
        "public_admin_vdem",
        "socgr_civ_libs_vdem",
        "dom_auton_vdem",
        "int_auton_vdem",
        "socgr_pow_vdem",
        "priv_libs_vdem",
        "pol_libs_vdem",
        # row indicators
        "transplaws_row",
        "accessjust_w_row",
        "accessjust_m_row",
        "electfreefair_row",
        "electmulpar_row",
    ]
    citation_mcmann = [
        "corruption_vdem",
        "corr_publsec_vdem",
        "corr_exec_vdem",
    ]
    citation_teorell = [
        "electdem_vdem",
        # row indicators
        "electdem_dich_row_owid",
    ]

    citation_full = {
        # Single citations
        **{i: CITATION_COPPEDGE for i in citation_coppedge},
        **{i: CITATION_PEMSTEIN for i in citation_pemstein},
        **{i: CITATION_MCMANN for i in citation_mcmann},
        **{i: CITATION_TEORELL for i in citation_teorell},
        ##
        "egaldem_vdem": CITATION_SIGMAN,
        "wom_emp_vdem": CITATION_SUNDSTROM,
        "wom_pol_par_vdem": CITATION_SUNDSTROM,
        # Combined citations
        "egal_vdem{dim}": f"{CITATION_SIGMAN};\n\n {CITATION_COPPEDGE}",
        "equal_rights_vdem{dim}": f"{CITATION_SIGMAN};\n\n {CITATION_PEMSTEIN}",
        "equal_access_vdem{dim}": f"Sigman and Lindberg (2017);\n\n {CITATION_PEMSTEIN}",
        ##
        "equal_res_vdem{dim}": f"{CITATION_SIGMAN};\n\n {CITATION_PEMSTEIN}",
        "personalism_vdem{dim}": "Sigman and Lindberg (2017, V-Dem Working Paper Series 2017:56); Sigman and Lindberg (2018); {CITATION_PEMSTEIN}",
        "wom_civ_libs_vdem{dim}": f"{CITATION_SUNDSTROM};\n\n {CITATION_PEMSTEIN}",
        "wom_civ_soc_vdem{dim}": f"{CITATION_SUNDSTROM};\n\n {CITATION_PEMSTEIN}",
    }
    citation_full = {col: citation for col, citation in citation_full.items() if col in tb.columns}
    for indicator_name, citation_additional in citation_full.items():
        tb[indicator_name].metadata.origins[0].citation_full += f";\n\n{citation_additional}"

    # Add citation for Luhrmann (at the beginning of the citation full)
    citation_luhrmann = [
        "transplaws_row",
        "accessjust_w_row",
        "accessjust_m_row",
        "lib_dich_row",
        "electfreefair_row",
        "electdem_dich_row_owid",
        "electmulpar_row",
        "electmulpar_hoe_row_owid",
        "electmulpar_leg_row",
    ]
    citation_luhrmann = [col for col in citation_luhrmann if col in tb.columns]
    for indicator_name in citation_luhrmann:
        tb[indicator_name].metadata.origins[0].citation_full = (
            f"{CITATION_LUHRMANN};\n\n" + tb[indicator_name].metadata.origins[0].citation_full
        )
    return tb
