"""Load a meadow dataset and create a garden dataset."""

from typing import cast

import vdem_clean as clean  # VDEM's cleaning library
import vdem_impute as impute  # VDEM's imputing library
import vdem_refine as refine  # VDEM's imputing library
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # %% Load data
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("vdem")

    # Read table from meadow dataset.
    tb = ds_meadow["vdem"].reset_index()
    tb = cast(Table, tb.astype({"v2exnamhos": str}))

    #
    # Process data.
    #

    # %% PART 1: CLEAN
    # The following lines (until "PART 2") are the cleaning steps.
    # This is a transcription from Bastian's work: https://github.com/owid/notebooks/blob/main/BastianHerre/democracy/scripts/vdem_row_clean.do

    tb = clean.run(tb)

    # %% PART 2: IMPUTE
    # The following lines concern imputing steps.
    # Equivalent to: https://github.com/owid/notebooks/blob/main/BastianHerre/democracy/scripts/vdem_row_impute.do

    tb = impute.run(tb)

    # %% PART 3: REFINE
    tb = refine.run(tb)

    # %% Tweak citation full for some indicators
    tb = add_citation_full(tb)

    # %% Set index
    tb = tb.format()

    # %% Save
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


# %%
def add_citation_full(tb: Table) -> Table:
    """Add additional citations.

    Some indicators require additional citation information.
    """
    CITATION_COPPEDGE = "Coppedge et al. (2015, V-Dem Working Paper Series 2015:6)"
    CITATION_PEMSTEIN = "Pemstein et al. (2024, V-Dem Working Paper Series 2024:21)"
    CITATION_SIGMAN = "Sigman et al. (2015, V-Dem Working Paper Series 2015:22)"
    DIMENSIONS = ["", "_low", "_high"]
    citation_coppedge = [
        *[f"libdem_vdem{dim}" for dim in DIMENSIONS],
        *[f"participdem_vdem{dim}" for dim in DIMENSIONS],
        *[f"delibdem_vdem{dim}" for dim in DIMENSIONS],
        *[f"lib_vdem{dim}" for dim in DIMENSIONS],
        *[f"particip_vdem{dim}" for dim in DIMENSIONS],
        *[f"delib_vdem{dim}" for dim in DIMENSIONS],
    ]
    citation_pemstein = [
        *[f"freeexpr_vdem{dim}" for dim in DIMENSIONS],
        *[f"freeassoc_vdem{dim}" for dim in DIMENSIONS],
        *[f"electfreefair_vdem{dim}" for dim in DIMENSIONS],
        *[f"indiv_libs_vdem{dim}" for dim in DIMENSIONS],
        *[f"judicial_constr_vdem{dim}" for dim in DIMENSIONS],
        *[f"legis_constr_vdem{dim}" for dim in DIMENSIONS],
        *[f"civsoc_particip_vdem{dim}" for dim in DIMENSIONS],
        *[f"corr_leg_vdem{dim}" for dim in DIMENSIONS],
        *[f"justified_polch_vdem{dim}" for dim in DIMENSIONS],
        *[f"justcomgd_polch_vdem{dim}" for dim in DIMENSIONS],
        *[f"counterarg_polch_vdem{dim}" for dim in DIMENSIONS],
        *[f"elitecons_polch_vdem{dim}" for dim in DIMENSIONS],
        *[f"soccons_polch_vdem{dim}" for dim in DIMENSIONS],
        *[f"corr_jud_vdem{dim}" for dim in DIMENSIONS],
        *[f"public_admin_vdem{dim}" for dim in DIMENSIONS],
        *[f"socgr_civ_libs_vdem{dim}" for dim in DIMENSIONS],
        *[f"dom_auton_vdem{dim}" for dim in DIMENSIONS],
        *[f"int_auton_vdem{dim}" for dim in DIMENSIONS],
        *[f"socgr_pow_vdem{dim}" for dim in DIMENSIONS],
        *[f"priv_libs_vdem{dim}" for dim in DIMENSIONS],
        *[f"pol_libs_vdem{dim}" for dim in DIMENSIONS],
    ]
    citation_full = {
        **{f"electdem_vdem{dim}": "Teorell et al. (2019)" for dim in DIMENSIONS},
        **{f"egaldem_vdem{dim}": CITATION_SIGMAN for dim in DIMENSIONS},
        **{f"egal_vdem{dim}": f"{CITATION_SIGMAN}; {CITATION_COPPEDGE}" for dim in DIMENSIONS},
        **{f"equal_rights_vdem{dim}": f"{CITATION_SIGMAN}; {CITATION_PEMSTEIN}" for dim in DIMENSIONS},
        **{f"equal_access_vdem{dim}": f"Sigman and Lindberg (2017); {CITATION_PEMSTEIN}" for dim in DIMENSIONS},
        **{f"equal_res_vdem{dim}": f"{CITATION_SIGMAN}; {CITATION_PEMSTEIN}" for dim in DIMENSIONS},
        **{
            f"description_from_producer{dim}": "Sigman and Lindberg (2017, V-Dem Working Paper Series 2017:56); Sigman and Lindberg (2018); {CITATION_PEMSTEIN}"
            for dim in DIMENSIONS
        },
        **{i: CITATION_COPPEDGE for i in citation_coppedge},
        **{i: CITATION_PEMSTEIN for i in citation_pemstein},
    }
    for indicator_name, citation_additional in citation_full.items():
        tb[indicator_name].metadata.origins[0].citation_full += f";\n\n{citation_additional}"

    return tb
