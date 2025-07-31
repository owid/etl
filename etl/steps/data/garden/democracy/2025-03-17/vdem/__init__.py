"""Load a meadow dataset and create a garden dataset.

NOTES: there seems to be some values for indicator `turnout_total_vdem` (i.e. `v2elvaptrn`) that exceed 100% (e.g. Gabon@1986, Somalia@1979, etc.).

You can check by running:

```python
tb.sort_values("v2elvaptrn", ascending=False)[["country", "year", "v2elvaptrn"]].head(50)
```

Unclear why this occurs, but leads to sudden jumps for region aggregates, e.g. for Africa in 1986 (chart_id=7777).
"""

from typing import List

import vdem_aggregate as aggregate  # VDEM's aggregating library
import vdem_clean as clean  # VDEM's cleaning library
import vdem_impute as impute  # VDEM's imputing library
import vdem_refine as refine  # VDEM's imputing library
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Number of countries as of 2024
NUM_COUNTRIES_2024 = 179

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
    "wom_hoe_ever",
    "wom_hoe_ever_dem",
    "v2exfemhoe",
    "regime_imputed",
    "num_years_in_electdem_consecutive",
    "num_years_in_libdem_consecutive",
    "num_years_in_electdem",
    "num_years_in_libdem",
    "num_years_in_electdem_consecutive_cat",
    "num_years_in_libdem_consecutive_cat",
]


def run() -> None:
    # %% Load data
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("vdem")
    ds_regions = paths.load_dataset("regions")
    ds_population = paths.load_dataset("population")

    # Read table from meadow dataset.
    tb = ds_meadow.read("vdem").astype({"v2exnamhos": "string"})

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
    tb = clean.run(tb, country_mapping_path=paths.country_mapping_path)

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
    (
        tb_uni_without_regions,
        tb_uni_with_regions,
        tb_multi_without_regions,
        tb_multi_with_regions,
        tb_countries_counts,
        tb_population_counts,
    ) = aggregate.run(tb, ds_regions, ds_population)

    # %% PART 4B: Share of population
    paths.log.info("4B/ Share of countries...")
    num_countries = tb_uni_without_regions.loc[
        tb_uni_without_regions["year"] == tb_uni_without_regions["year"].max()
    ].shape[0]
    tb_countries_share = estimate_share_countries(tb_countries_counts, num_countries_last=num_countries)

    # %% PART 5: Format and prepare tables
    paths.log.info("5/ Formatting tables...")
    tb_meta = tb.loc[:, ["year", "country", "regime_imputed", "regime_imputed_country", "histname"]]
    tb_meta = tb_meta.format(short_name="metadata")

    tb_uni_without_regions = tb_uni_without_regions.format(
        keys=["country", "year"],
        short_name="vdem_uni_without_regions",
    )
    tb_uni_with_regions = tb_uni_with_regions.format(
        keys=["country", "year"],
        short_name="vdem_uni_with_regions",
    )
    tb_multi_without_regions = tb_multi_without_regions.format(
        keys=["country", "year", "estimate"], short_name="vdem_multi_without_regions"
    )
    tb_multi_with_regions = tb_multi_with_regions.format(
        keys=["country", "year", "estimate"], short_name="vdem_multi_with_regions"
    )
    tb_countries_counts = tb_countries_counts.format(
        keys=["country", "year", "category"], short_name="vdem_num_countries"
    )
    tb_countries_share = tb_countries_share.format(keys=["country", "year"], short_name="vdem_share_countries")
    tb_population_counts = tb_population_counts.format(
        keys=["year", "country", "category"], short_name="vdem_population"
    )

    # %% PART 6: Sanity checks
    # We add a note in the description_key of certain indicators (look for "&key_regions" in vdem.meta.yml). This note should only be added to indicators that have data pre-1900. We have manually removed this note from the affected indicators. Here, we just check that the indicators that shouldn't have this note, continue not to have data before 1900.

    def _check_1900(tb, cols):
        # Get all columns except 'country' and 'year'
        data_columns = [col for col in tb.columns if col not in ["country", "year"]]

        # Calculate first year with data for each column using melt + groupby
        tb_ = tb.reset_index()
        melted = tb_[["year"] + data_columns].melt(
            id_vars=["year"], value_vars=data_columns, var_name="indicator", value_name="value"
        )
        first_years = melted.dropna(subset=["value"]).groupby("indicator")["year"].min()
        cols_found = set(first_years.loc[first_years >= 1900].index)

        assert cols == cols_found

    _check_1900(
        tb_uni_without_regions,
        {
            "corruption_cpi",
            "dirpop_vote_vdem",
            "goveffective_vdem_wbgi",
            "v2xpas_democracy",
            "v2xpas_democracy_government",
            "v2xpas_democracy_opposition",
            "wom_parl_vdem_cat",
        },
    )
    _check_1900(
        tb_multi_without_regions,
        {
            "counterarg_polch_vdem",
            "delib_vdem",
            "delibdem_vdem",
            "egal_vdem",
            "egaldem_vdem",
            "equal_res_vdem",
            "justcomgd_polch_vdem",
            "justified_polch_vdem",
            "v2caautmob",
            "v2cacamps",
            "v2cademmob",
            "v2cagenmob",
            "v2caviol",
            "v2mecorrpt",
            "v2smgovdom",
            "v2xca_academ",
            "wom_parl_vdem",
        },
    )

    # %% PART 7: Create list of tables
    tables = [
        # Metadata (former country names, etc.)
        tb_meta,
        # Main indicators (uni-dimensional) without regions
        tb_uni_without_regions,  # some have 0 origins
        # Main indicators (uni-dimensional) with regions
        tb_uni_with_regions,
        # Main indicators (multi-dimensional) without regions
        tb_multi_without_regions,
        # Main indicators (multi-dimensional) with regions
        tb_multi_with_regions,
        # Number of countries with X properties
        tb_countries_counts,
        # Share of countries with X properties
        tb_countries_share,
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
    tables = adjust_citation_full(tables)

    # %% Set index
    # tb = tb.format()

    # %% Save
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=tables, check_variables_metadata=True, default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


# %%
def adjust_citation_full(tbs: List[Table]) -> List[Table]:
    """Adjust the citation_full metadata field for some indicators."""
    for tb in tbs:
        tb = replace_citation_full(tb)
        tb = append_citation_full(tb)
    return tbs


def replace_citation_full(tb: Table) -> Table:
    """Replace the citation_full metadata field for some indicators."""
    return tb


def append_citation_full(tb: Table) -> Table:
    """Add additional citations.

    Some indicators require additional citation information.
    """
    CITATION_COPPEDGE = "Coppedge et al. (2015), 'Measuring High Level Democratic Principles using the V-Dem Data', V-Dem Working Paper Series 2015(6)"
    # CITATION_PEMSTEIN = "Pemstein et al. (2024, V-Dem Working Paper Series 2024:21)"
    CITATION_SIGMAN = "Sigman et al. (2015), 'The Index of Egalitarian Democracy and its Components: V-Dem's Conceptualization and Measurement', V-Dem Working Paper Series 2015(22)."
    CITATION_MCMANN = "McMann et al. (2016), 'Strategies of Validation: Assessing the Varieties of Democracy Corruption Data', V-Dem Working Paper Series 2016(23)."
    CITATION_SUNDSTROM = "Sundström et al. (2017), 'Women's Political Empowerment: A New Global Index, 1900-2012', World Development 94, 321-335"
    CITATION_TEORELL = "Teorell et al. (2019), 'Measuring Polyarchy Across the Globe, 1900-2017', Studies in Comparative International Development 54(1), 71-95."
    CITATION_LUHRMANN = "Lührmann, Anna, Marcus Tannnberg, and Staffan Lindberg. 2018. Regimes of the World (RoW): Opening New Avenues for the Comparative Study of Political Regimes. Politics and Governance 6(1): 60-77."
    CITATION_HELLMEIER = "Hellmeier and Bernhard (2022), 'Mass Mobilization and Regime Change. Evidence From a New Measure of Mobilization for Democracy and Autocracy From 1900 to 2020', V-Dem Working Paper Series 2022(128)."
    CITATION_ANGIOLILLO = (
        "Agiolillo et al. (2023), 'Democractic-Autocratic Party Systems: A New Index', V-Dem Working Paper Series (143)"
    )

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
    # citation_pemstein = [
    #     "freeexpr_vdem",
    #     "freeassoc_vdem",
    #     "electfreefair_vdem",
    #     "indiv_libs_vdem",
    #     "judicial_constr_vdem",
    #     "legis_constr_vdem",
    #     "civsoc_particip_vdem",
    #     "corr_leg_vdem",
    #     "justified_polch_vdem",
    #     "justcomgd_polch_vdem",
    #     "counterarg_polch_vdem",
    #     "elitecons_polch_vdem",
    #     "soccons_polch_vdem",
    #     "corr_jud_vdem",
    #     "public_admin_vdem",
    #     "socgr_civ_libs_vdem",
    #     "dom_auton_vdem",
    #     "int_auton_vdem",
    #     "socgr_pow_vdem",
    #     "priv_libs_vdem",
    #     "pol_libs_vdem",
    #     # row indicators
    #     "transplaws_row",
    #     "accessjust_w_row",
    #     "accessjust_m_row",
    #     "electfreefair_row",
    #     "electmulpar_row",
    # ]
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
    citation_helmmeier = [
        "v2cademmob",
        "v2caautmob",
    ]
    citation_angiolillo = [
        "v2xpas_democracy",
        "v2xpas_democracy_government",
        "v2xpas_democracy_opposition",
    ]

    citation_full = {
        # Single citations
        **{i: CITATION_COPPEDGE for i in citation_coppedge},
        # **{i: CITATION_PEMSTEIN for i in citation_pemstein},
        **{i: CITATION_MCMANN for i in citation_mcmann},
        **{i: CITATION_TEORELL for i in citation_teorell},
        **{i: CITATION_HELLMEIER for i in citation_helmmeier},
        **{i: CITATION_ANGIOLILLO for i in citation_angiolillo},
        ##
        "egaldem_vdem": CITATION_SIGMAN,
        "wom_emp_vdem": CITATION_SUNDSTROM,
        "wom_pol_par_vdem": CITATION_SUNDSTROM,
        # Combined citations
        "egal_vdem{dim}": f"{CITATION_SIGMAN};\n\n {CITATION_COPPEDGE}",
        "equal_rights_vdem{dim}": f"{CITATION_SIGMAN}",
        "equal_access_vdem{dim}": "Sigman and Lindberg (2017)",
        # "equal_rights_vdem{dim}": f"{CITATION_SIGMAN};\n\n {CITATION_PEMSTEIN}",
        # "equal_access_vdem{dim}": f"Sigman and Lindberg (2017);\n\n {CITATION_PEMSTEIN}",
        ##
        "equal_res_vdem{dim}": f"{CITATION_SIGMAN}",
        # "equal_res_vdem{dim}": f"{CITATION_SIGMAN};\n\n {CITATION_PEMSTEIN}",
        "personalism_vdem{dim}": "Sigman and Lindberg (2017); Sigman and Lindberg (2018)",
        "wom_civ_libs_vdem{dim}": f"{CITATION_SUNDSTROM}",
        "wom_civ_soc_vdem{dim}": f"{CITATION_SUNDSTROM}",
        # "wom_civ_libs_vdem{dim}": f"{CITATION_SUNDSTROM};\n\n {CITATION_PEMSTEIN}",
        # "wom_civ_soc_vdem{dim}": f"{CITATION_SUNDSTROM};\n\n {CITATION_PEMSTEIN}",
    }
    citation_full = {col: citation for col, citation in citation_full.items() if col in tb.columns}
    for indicator_name, citation_additional in citation_full.items():
        if indicator_name in tb.columns:
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
        if indicator_name in tb.columns:
            tb[indicator_name].metadata.origins[0].citation_full = (
                f"{CITATION_LUHRMANN};\n\n" + tb[indicator_name].metadata.origins[0].citation_full
            )
    return tb


def estimate_share_countries(tb: Table, num_countries_last) -> Table:
    """Estimate the share of countries with a certain property."""
    # NOTE: The count of countries only considers *actually* existing countries, and skips imputed countries. That's due to how `aggregate.run` has implemented that. Therefore, we can estimate the share of countries easily by num_countries_women_ever / total_countries * 100. No need to worry about counting imputed countries!
    # The share is estimated relative to the number of countries as of 2024, which is a different strategy compared to the rest of indicators. That's because the framing is "looking backwards at the history of current countris".

    assert num_countries_last == NUM_COUNTRIES_2024, "The number of countries should be 179 as of 2024."

    columns_rename = {
        "num_countries_wom_hoe_ever": "share_countries_wom_hoe_ever",
        "num_countries_wom_hoe_ever_demelect": "share_countries_wom_hoe_ever_demelect",
    }
    columns = list(columns_rename.keys())
    tb_share = (
        tb.loc[:, ["country", "year", "category"] + columns]
        .copy()
        .dropna(how="all", subset=columns)
        .rename(columns=columns_rename)
    )

    # Keep only category "yes", and entity "World"
    tb_share = tb_share[(tb_share["category"] == "yes") & (tb_share["country"] == "World")].drop(columns=["category"])

    # Add a column with the total count of countries per year-country
    tb_share["total_countries"] = tb_share.groupby(["country", "year"], as_index=False)[
        "share_countries_wom_hoe_ever"
    ].transform("sum")

    assert tb_share["total_countries"].notna().all(), "NA detected!"

    # Option 1: Share of countries relative to current number of countries
    # tb_share["share_countries_wom_hoe_ever"] /= tb_share["total_countries"] * 0.01
    # tb_share["share_countries_wom_hoe_ever_demelect"] /= tb_share["total_countries"] * 0.01
    tb_share = tb_share.drop(columns=["total_countries"])

    # Option 2: Share of countries relative to number of countries as of 2024
    tb_share["share_countries_wom_hoe_ever"] /= num_countries_last * 0.01
    tb_share["share_countries_wom_hoe_ever_demelect"] /= num_countries_last * 0.01

    return tb_share
