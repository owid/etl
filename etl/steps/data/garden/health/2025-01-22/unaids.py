"""Processing of UNAIDS data.

There are two main UNAIDS sources: EPI, GAM, KPA, NCPI. More details in the snapshot step.
"""

import numpy as np
import owid.catalog.processing as pr
import pandas as pd
import yaml
from owid.catalog import Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Logger
log = get_logger()

REGIONS_TO_ADD = [
    "North America",
    "South America",
    "Europe",
    "Africa",
    "Asia",
    "Oceania",
    "Low-income countries",
    "Upper-middle-income countries",
    "Lower-middle-income countries",
    "High-income countries",
    # "World",
]
# TODO: UNAIDS already provides estimates for "World" for certain indicators
# Combining it with ours can be confusing for the user: "which are from UNAIDS" and which not?
# Also, it is technically challenging to only group for those indicator-dimensions for which we don't have "World" estimates by UNAIDS.
# A solution could be to label UNAIDS' with a different label, e.g. "World (UNAIDS)"

# tb.groupby("indicator").estimate.unique()
# None: AIDS_DEATHS, HIV_INCIDENCE, HIV_PREVALENCE, NEW_INFECTIONS, PLWH
# central 2: AIDS_MORTALITY_1000_POP
COLUMNS_RENAME_EPI = {
    "mother_dropped_off_art_during_breastfeeding_child_infected_": "art_drop_bf_child_inf",
    "mother_dropped_off_art_during_pregnancy_child_infected_duri": "art_drop_preg_child_inf",
    "started_art_before_the_pregnancy_child_infected_during_brea": "art_start_before_preg_child_inf_bf",
    "started_art_before_the_pregnancy_child_infected_during_preg": "art_start_before_preg_child_inf_preg",
    "started_art_during_in_pregnancy_child_infected_during_breas": "art_start_during_preg_child_inf_bf",
    "started_art_during_in_pregnancy_child_infected_during_pregn": "art_start_child_inf_during_preg",
    "started_art_late_in_pregnancy_child_infected_during_breastf": "art_start_late_preg_child_inf_bf",
    "started_art_late_in_pregnancy_child_infected_during_pregnan": "art_start_late_preg_child_inf_preg",
    "did_not_receive_art_during_pregnancy_child_infected_during_": "art_none_preg_child_inf_preg",
    "no_receive_art_during_bf": "art_none_bf",
    "percent_on_art_vl_suppressed": "percent_art_vl_suppressed",
    "mother_infected_during_breastfeeding_child_infected_during_": "mother_child_inf_bf",
    "mother_infected_during_pregnancy_child_infected_during_preg": "mother_child_inf_preg",
}
# GAM
INDICATORS_GAM_CATEGORICAL = [
    "Q_A_120",
    "Q_A_121",
    "Q_A_123",
    "Q_A_125",
    "Q_A_65",
    "Q_A_67",
    "Q_A_68",
    "Q_A_6B",
    "Q_A_70",
]
INDICATORS_GAM_DROP = [
    "INCOME_STATUS",
    "UNAIDS_RSTS",
    "GEOGRAPHICAL_REGIONS",
    "COUNTRY_OFFICES",
    "PROGRAMME_CERVICAL_CANCER",  # Unclear metadata
    "SURVEY_CERVICAL_CANCER",  # Unclear metadata
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    ## Load meadow dataset.
    ds_meadow = paths.load_dataset("unaids")
    # Load population dataset.
    # ds_population = paths.load_dataset("population")
    ## Load regions dataset.
    ds_regions = paths.load_dataset("regions")
    ## Load income groups dataset.
    ds_income_groups = paths.load_dataset("income_groups")

    # Load dimensions
    with paths.side_file("unaids.dimensions.yml").open() as f:
        dimensions = yaml.safe_load(f)

    with paths.side_file("unaids.indicators_to_dimensions.yml").open() as f:
        dimensions_collapse = yaml.safe_load(f)

    # Load list with all countries
    countries_all = get_all_countries(ds_regions, ds_income_groups)

    ###############################
    # EPI data
    ###############################
    tb_epi = ds_meadow.read("epi")  ## 1,687,587 rows
    tb_epi = make_table_epi(
        tb=tb_epi,
        dimensions=dimensions["epi"],
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        countries_all=countries_all,
    )
    tb_epi = tb_epi.format(["country", "year", "age", "sex", "estimate"], short_name="epi")

    ###############################
    # GAM data
    ###############################
    tb = ds_meadow.read("gam")  # 128,614 rows

    # Handle countries: Harmonize, drop non-countries, etc.
    paths.log.info("GAM: processing countries")
    tb = handle_countries_gam(tb)

    # Drop non-relevant (or non-supported) indicators
    tb = tb.loc[~tb["indicator"].isin(INDICATORS_GAM_DROP + INDICATORS_GAM_CATEGORICAL)]
    tb = tb.loc[~((tb["indicator"] == "POPULATION") & (tb["dimension"] == "TOTAL"))]

    # Handle dimensions: Expand raw dimensions, group indicators, etc.
    paths.log.info("GAM: initial processing of dimensions")
    tb = handle_dimensions_clean_gam(tb, dimensions, dimensions_collapse["gam"])

    # DTYPE: Make value float
    tb["value"] = tb["value"].astype(float)

    # SEPARATE DATA
    paths.log.info("GAM: creating multiple tables")
    # 1/ [sex, age, group, hepatitis]. Separate hepatitis data.
    tb_hepatitis, mask_hepatitis = extract_hepatitis_table_gam(tb)

    # 2.1/ [estimate]. Separate estimate data.
    tb_estimate, mask_estimate = extract_estimate_table_gam(tb)
    # 2.2/ [group]. Separate data with no sex and no age.
    tb_group, mask_only_group = extract_group_table_gam(tb)
    # 2.3/ [age]. Separate data with no sex and group.
    tb_age, mask_only_age = extract_age_table_gam(tb)
    # 2.4/ [sex]. Separate data with no age and group.
    tb_sex, mask_only_sex = extract_sex_table_gam(tb)

    # 3.1/ [age, sex]. Separate data with NO group data.
    tb_age_sex, mask_no_group = extract_age_sex_table_gam(tb)
    # 3.2/ [age, group]. Separate data with NO sex.
    tb_age_group, mask_no_sex = extract_age_group_table_gam(tb)

    # 4/ []. Separate data with NO dimension.
    tb_no_dim, mask_no_dim = extract_no_dim_table_gam(tb)

    # 5/ Drop separated data from main table
    tb = tb.loc[
        ~(
            mask_hepatitis
            | mask_estimate
            | mask_only_group
            | mask_only_age
            | mask_only_sex
            | mask_no_group
            | mask_no_sex
            | mask_no_dim
        )
    ]
    assert tb["hepatitis"].isna().all()
    assert tb["estimate"].isna().all()
    tb = tb.drop(columns=["hepatitis", "estimate"])

    # DIMENSIONS
    paths.log.info("GAM: processing dimensions")
    # 1/ Incorporate dimensional information from `dimension_0` into `group` and drop `dimension_0`
    tb = incorporate_dimension_0(tb)

    # 2/ Ensure values in dimension `sex` (avoid None values)
    tb = handle_nulls_in_dimensions_gam(tb)

    # 3/ Get table with only `sex` and `group` dimensions.
    # This was not done in step "SEPARATE DATA", because we want to benefit from some of the processing done in this step. In particular, of the combination of `dimension_0` and `group`.

    # 3.3/ [sex, group]. Separate data with NO age
    tb_sex_group, mask_no_age = extract_sex_group_table_gam(tb)
    ## Correction
    mask = (tb_sex_group.country == "China") & (tb_sex_group.year == 2021)
    x = tb_sex_group.loc[mask]
    assert len(x) == 2
    assert set(x["value"].unique()) == {4.1}
    assert set(x["dimension"].unique()) == {"TOTAL", "ALL_AGES"}
    tb_sex_group = tb_sex_group.loc[
        ~(
            (tb_sex_group["country"] == "China")
            & (tb_sex_group["year"] == 2021)
            & (tb_sex_group["dimension"] == "TOTAL")
        )
    ]
    # Remove from main table!
    tb = tb.loc[~mask_no_age]

    # RESHAPE (and check)
    paths.log.info("GAM: Format (and check)")

    def pivot_and_format(tb, columns, short_name, regions_agg=True):
        # 1/ Save some fields (this might be useful later)
        tb_meta = tb[["indicator", "unit"]].drop_duplicates()
        assert not tb_meta[["indicator"]].duplicated().any(), "Multiple units for a single indicator!"

        # 2/ Pivot
        tb = tb.pivot(index=columns, columns="indicator", values="value").reset_index()
        tb = tb.underscore()
        tb = tb.dropna(how="all")

        # (OPTIONAL) Add regional aggregates
        # if columns_agg is not None:
        if regions_agg:
            columns_agg = tb_meta.loc[tb_meta["unit"] == "NUMBER", "indicator"].tolist()
            # columns_agg = []
            if columns_agg != []:
                tb = add_regional_aggregates(
                    tb,
                    ds_regions,
                    ds_income_groups,
                    columns_agg=columns_agg,
                    countries_all=countries_all,
                )
        # 3/ Format
        tb = tb.format(columns, short_name=short_name)

        return tb

    # DEBUGGING
    # tbr = tb.copy()

    tb = pivot_and_format(tb, ["country", "year", "age", "sex", "group"], "gam_age_sex_group")
    tb_hepatitis = pivot_and_format(
        tb_hepatitis, ["country", "year", "age", "sex", "group", "hepatitis"], "gam_hepatitis"
    )
    tb_estimate = pivot_and_format(
        tb_estimate,
        ["country", "year", "estimate"],
        "gam_estimates",
    )
    tb_group = pivot_and_format(tb_group, ["country", "year", "group"], "gam_group")
    tb_age = pivot_and_format(tb_age, ["country", "year", "age"], "gam_age")
    tb_sex = pivot_and_format(tb_sex, ["country", "year", "sex"], "gam_sex")
    tb_age_sex = pivot_and_format(tb_age_sex, ["country", "year", "age", "sex"], "gam_age_sex")
    tb_age_group = pivot_and_format(tb_age_group, ["country", "year", "age", "group"], "gam_age_group")
    tb_sex_group = pivot_and_format(tb_sex_group, ["country", "year", "sex", "group"], "gam_sex_group")
    tb_no_dim = pivot_and_format(
        tb_no_dim,
        ["country", "year"],
        "gam",
    )

    # SCALING
    tb_no_dim["resource_needs_ft"] *= 1e6
    tb_group["resource_avail_constant"] *= 1e6

    # SANITY CHECK
    assert set(tb.columns) == {
        "art_coverage",
        "avoidance_care",
        "condom_use",
        "discriminatory_attitudes",
        "experience_stigma",
        "hiv_prevalence",
        "hiv_programmes_coverage",
        "hiv_status_awareness",
        "hiv_tests",
        "pwid_safety",
    }

    # TABLE GROUPS
    paths.log.info("GAM: pivoting and formatting")
    tables_gam = [
        tb,
        tb_hepatitis,
        tb_estimate,
        tb_group,
        tb_age,
        tb_sex,
        tb_age_sex,
        tb_age_group,
        tb_sex_group,
        tb_no_dim,
    ]

    # FOR DEBUGGING
    # tb_meta = tbr[["indicator", "indicator_description", "unit"]].drop_duplicates().sort_values("indicator")
    # # for _, row in tb_meta.iterrows():
    # #     print(
    # #         f"{row.indicator}:\n\ttitle: {row.indicator_description}\n\tunit: {row['unit']}\n\tdescription_short: ''\n\tdescription_from_producer: ''"
    # #     )
    # tb_meta

    # x = tb.copy()
    # cols_idx = list(x.index.names)
    # x = x.reset_index()
    # x = x.melt(id_vars=cols_idx).dropna(subset="value", axis=0)
    # cols = [col for col in cols_idx if col not in ["country", "year"]]
    # x = x[["variable"] + cols].drop_duplicates().sort_values(["variable"] + cols)
    # x

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    tables = [
        tb_epi,
        *tables_gam,
    ]
    ds_garden = create_dataset(
        dest_dir,
        tables=tables,
        default_metadata=ds_meadow.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def make_table_epi(tb, dimensions, ds_regions, ds_income_groups, countries_all):
    # Add dimensions
    tb = expand_raw_dimension(tb, dimensions)

    # Harmonize indicator names
    tb["indicator"] = tb["indicator"].str.lower().apply(lambda x: COLUMNS_RENAME_EPI.get(x, x))

    # Sanity checks
    ## Check source
    assert set(tb["source"].unique()) == {"UNAIDS_Estimates_"}, "Unexpected source!"
    ## No textual data
    assert not tb["is_text"].any(), "Some data is textual!"
    ## Chec no NaNs
    assert not tb.value.isna().any(), "Some NaNs were detected, but none were expected!"

    # Save some fields (this might be useful later)
    tb_meta = tb[["indicator", "indicator_description", "unit"]].drop_duplicates()
    assert not tb_meta["indicator"].duplicated().any(), "Multiple descriptions or units for a single indicator!"

    # Harmonize
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
        excluded_countries_file=paths.excluded_countries_path,
    )

    # Pivot table (and lower-case columns)
    tb = tb.pivot(
        index=["country", "year", "age", "sex", "estimate"], columns="indicator", values="value"
    ).reset_index()
    tb = tb.underscore()

    # Rename columns
    tb = tb.rename(columns=COLUMNS_RENAME_EPI)

    # Add older ART-prevented deaths data
    tb = add_old_art_averted_deaths_data(tb)

    # Drop all NaN rows
    tb = tb.dropna(how="all")

    # Add regional data
    columns_agg = tb_meta.loc[tb_meta["unit"] == "NUMBER", "indicator"].tolist()
    if columns_agg != []:
        tb = add_regional_aggregates(
            tb,
            ds_regions,
            ds_income_groups,
            columns_agg=columns_agg,
            countries_all=countries_all,
        )

    return tb


def handle_countries_gam(tb):
    territories_extra = {
        "Liechtenstein",
        "Holy See",
        "Saint-Martin (French part)",
        "Martinique",
        "Saint Helena",
        "French Guiana",
        "Isle of Man",
        "Guadeloupe",
        "Western Sahara",
        "Jersey",
        "Guernsey",
    }
    # Drop areas that are not countries (we look for areas appearing very few times)
    territories = tb["country"].value_counts().sort_values(ascending=False)
    territories = set(territories[territories >= 88].index)
    tb = tb.loc[(tb["country"].isin(territories | territories_extra))]

    # Harmonize
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
        excluded_countries_file=paths.excluded_countries_path,
        make_missing_countries_nan=True,
    )

    return tb


def handle_dimensions_clean_gam(tb, dimensions, dimensions_collapse_gam):
    # Collapse original indicators into indicator + dimension
    dix = {}
    for dim in dimensions_collapse_gam:
        _dix = {k: {**v, "name": dim["name"]} for k, v in dim["indicators_origin"].items()}
        dix |= _dix
    tb["dimension_0"] = tb["indicator"].map(lambda x: dix[x]["dimension"] if x in dix else None)
    tb["indicator"] = tb["indicator"].map(lambda x: dix[x]["name"] if x in dix else x)
    ## Lower case indicator names
    tb["indicator"] = tb["indicator"].str.lower()

    # Expand original dimension into multiple dimensions
    tb = expand_raw_dimension(
        tb, dimensions["gam"], dimension_names=["sex", "age", "group", "hepatitis", "estimate"], drop=False
    )

    # POPULATION: Fix population data (this is done here to avoid it being separated by `extract_estimate_table_gam`)
    mask = tb["indicator"] == "population"
    assert set(tb.loc[mask, "estimate"].unique()) == {"central"}, "Unexpected estimate!"
    tb.loc[mask, "estimate"] = None

    # SEX: set sex="male" and sex="others" when applicable
    ### Set to "male"
    mask = (
        (tb["dimension_0"] == "men who have sex with men")
        & (tb["indicator"] != "condoms_distributed_pp")
        & (tb["indicator"] != "population")
    )
    tb.loc[mask, "sex"] = "male"

    ### Set to "other"
    tb.loc[
        (tb["group"].astype("string").str.contains("trans"))
        | (tb["dimension_0"].astype("string").str.contains("trans")),
        "sex",
    ] = "other"

    # When `group` is in {"procured", "distributed"}, and dimension_0 == "self-tests"
    ## Solution: I just change the indicator name (incorporate dimension_0 into indicator)
    mask = tb["group"].isin(["procured", "distributed"])
    assert set(tb.loc[mask, "indicator"].unique()) == {"hiv_tests"}
    assert set(tb.loc[mask, "dimension_0"].unique()) == {"self-tests"}
    tb.loc[mask, "indicator"] = "hiv_self_tests"
    tb.loc[mask, "dimension_0"] = np.nan

    # denied_services_hiv_status should be "discrimination_hc_settings", with `group`="total"
    tb.loc[tb["indicator"] == "denied_services_hiv_status", "indicator"] = "discrimination_hc_settings"

    return tb


def extract_hepatitis_table_gam(tb):
    """Separate hepatitis data from main table.

    Why? Hepatitis data contains extra dimension not needed for the rest of data.

    Affected indicator should only be `viral_hepatitis`.
    """
    # Get main hepatitis data
    indicators_hepatitis = set(tb.dropna(subset="hepatitis")["indicator"].unique())
    assert indicators_hepatitis == {"viral_hepatitis"}, "Unexpected extra indicators!"
    mask_1 = tb["indicator"].isin(indicators_hepatitis)
    tb_hepatitis = tb.loc[mask_1]
    # Get also hepatitis C for prisoners (for some reason comes separately)
    mask_2 = tb["indicator"] == "prisoners_hepatitis_c"
    tb_hepatitis_2 = tb.loc[mask_2]
    tb_hepatitis_2["indicator"] = "viral_hepatitis"
    tb_hepatitis_2["hepatitis"] = "C"
    tb_hepatitis_2["group"] = "prisoners"
    # Combine hepatitis data
    tb_hepatitis = pr.concat([tb_hepatitis, tb_hepatitis_2])

    # Checks
    assert set(tb_hepatitis["sex"].unique()) == {"female", "male", "other", "total"}
    assert set(tb_hepatitis["age"].unique()) == {"0-25", "25+", "total"}
    assert set(tb_hepatitis["hepatitis"].unique()) == {"B", "C"}
    assert set(tb_hepatitis["group"].unique()) == {None, "prisoners", "transgender"}
    assert tb_hepatitis["group"].notna().sum() == 52, "Unexpected not-NAs"

    # Drop columns
    tb_hepatitis = tb_hepatitis.drop(columns=["estimate"])

    # Fill NAs in group with dimension_0
    mask = tb_hepatitis["group"].isna()
    tb_hepatitis.loc[mask, "group"] = tb_hepatitis.loc[mask, "dimension_0"]
    tb_hepatitis.loc[mask, "dimension_0"] = np.nan

    # Fix values in `group` whenever there are values in both `group` and `dimension_0`
    mask = tb_hepatitis["dimension_0"].notna()
    assert set(tb_hepatitis.loc[mask, "dimension_0"].unique()) == {"people who inject drugs"}
    assert set(tb_hepatitis.loc[mask, "group"].unique()) == {"transgender"}
    tb_hepatitis.loc[mask, "group"] = "transgender, people who inject drugs"
    tb_hepatitis.loc[mask, "dimension_0"] = np.nan

    # Check
    assert tb_hepatitis["dimension_0"].isna().all(), "Pending values in dimension_0 to incorporate into group!"
    assert tb_hepatitis[["group", "sex", "age"]].notna().all().all(), "Pending None values!!"

    # Create mask
    mask_hepatitis = mask_1 | mask_2

    return tb_hepatitis, mask_hepatitis


def extract_estimate_table_gam(tb):
    """Separate indicators with estimate data (low, high, central) from main table.

    Why? Because these indicators have extra dimension not needed for the rest of data.
    """
    indicators_estimate = set(tb.dropna(subset="estimate")["indicator"].unique())
    assert indicators_estimate == {
        "tb_related_deaths",
        "incident_tb_cases",
        "hiv_new_tb_cases",
        "comanagement_tb_hiv",
    }, "Unexpected extra indicators"
    mask_estimate = tb["indicator"].isin(indicators_estimate)
    tb_estimate = tb.loc[mask_estimate]

    # Checks
    assert set(tb_estimate["sex"].unique()) == {None, "total"}
    assert set(tb_estimate["age"].unique()) == {None, "total"}
    assert tb_estimate["group"].isna().all()
    assert tb_estimate["dimension_0"].isna().all()
    assert tb_estimate["estimate"].notna().all()

    # Drop columns
    tb_estimate = tb_estimate.drop(columns=["sex", "age", "group", "hepatitis"])

    return tb_estimate, mask_estimate


def extract_group_table_gam(tb):
    """Separate indicators with data without sex or age (only group)."""
    indicators_only_group = [
        "discrimination_hc_settings",
        "domestic_spending_fund_source",
        "resource_avail_constant",
        "condoms_distributed_pp",
        "population",
    ]
    mask_only_group = tb["indicator"].isin(indicators_only_group)
    tb_new = tb.loc[mask_only_group]

    # Fix group for indicator "condoms_distributed_pp"
    mask = tb_new["indicator"] == "condoms_distributed_pp"
    assert tb_new.loc[mask, "dimension_0"].notna().all()
    assert set(tb_new.loc[mask, "group"].unique()) == {None, "total"}
    tb_new.loc[mask, "group"] = tb_new.loc[mask, "dimension_0"]
    tb_new.loc[mask, "dimension_0"] = None

    # Fix group for indicator "population"
    mask = tb_new["indicator"] == "population"
    assert tb_new.loc[mask, "dimension_0"].notna().all()
    assert tb_new.loc[mask, "group"].isna().all()
    tb_new.loc[mask, "group"] = tb_new.loc[mask, "dimension_0"]
    tb_new.loc[mask, "dimension_0"] = None

    # Checks
    assert set(tb_new["sex"].unique()) == {None, "other", "total"}
    assert set(tb_new["age"].unique()) == {None, "total"}
    assert tb_new["hepatitis"].isna().all()
    assert tb_new["estimate"].isna().all()
    assert tb_new["group"].notna().all()
    assert set(tb_new.loc[tb_new["group"] == "total", "dimension"].unique()) == {"TOTAL"}
    assert tb_new["dimension_0"].isna().all()
    # Drop columns
    tb_new = tb_new.drop(columns=["sex", "age", "hepatitis", "estimate"])

    return tb_new, mask_only_group


def extract_age_table_gam(tb):
    """Separate indicators with data without sex or group (only age)."""
    indicators = {
        "male_circumcisions_performed",
    }
    mask = tb["indicator"].isin(indicators)
    tb_new = tb.loc[mask]
    # Checks
    assert tb_new["hepatitis"].isna().all()
    assert tb_new["estimate"].isna().all()
    assert tb_new["sex"].isna().all()
    assert tb_new["group"].isna().all()
    assert tb_new["age"].notna().all()
    assert tb_new["dimension"].notna().all()
    assert tb_new["dimension_0"].isna().all()
    # Drop columns
    tb_new = tb_new.drop(columns=["sex", "group", "hepatitis", "estimate"])

    return tb_new, mask


def extract_sex_table_gam(tb):
    """Separate indicators with data without age or group (only sex)."""
    indicators = {
        "knowledge_in_young_people",
    }
    mask = tb["indicator"].isin(indicators)
    tb_new = tb.loc[mask]
    # Checks
    assert tb_new["hepatitis"].isna().all()
    assert tb_new["estimate"].isna().all()
    assert set(tb_new["group"].unique()) == {"all questions"}
    assert set(tb_new["age"].unique()) == {"15-24"}
    assert set(tb_new["sex"].unique()) == {"female", "male", "total"}
    assert tb_new["dimension_0"].isna().all()
    # Drop columns
    tb_new = tb_new.drop(columns=["group", "age", "hepatitis", "estimate"])

    return tb_new, mask


def extract_age_sex_table_gam(tb):
    """Separate indicators with no group data (only sex and age)."""
    indicators_no_group = {
        "att_tow_wife_beating",
        "demand_family_planning",
        "hiv_pos_rate",
        "prevalence_ipv",
        "pwid_ost_coverage",
    }
    mask = tb["indicator"].isin(indicators_no_group)
    tb_new = tb.loc[mask]
    # Fill None values in `sex`
    # tb_new.loc[tb_new["sex"].isna(), "indicator"].unique()
    # _debug_highlight_none(tb_age_sex, "att_tow_wife_beating", "sex")
    tb_new = safe_replace_NAs(
        tb=tb_new,
        set_map={"pwid_ost_coverage": {"LESS_25", "25_AND_UP"}, "att_tow_wife_beating": {"ALL_AGES"}},
        dimension="sex",
        value="total",
    )
    # Checks
    assert set(tb_new["group"].unique()) == {None, "total"}
    assert tb_new["hepatitis"].isna().all()
    assert tb_new["estimate"].isna().all()
    assert tb_new["age"].notna().all()
    assert tb_new["sex"].notna().all()
    assert tb_new["dimension_0"].isna().all()
    # Drop columns
    tb_new = tb_new.drop(columns=["group", "hepatitis", "estimate"])

    return tb_new, mask


def extract_age_group_table_gam(tb):
    """Separate indicators with no sex data (only group and age)."""
    indicators = {
        "hiv_self_tests",
        "prevalence_male_circumcision",
    }
    mask = tb["indicator"].isin(indicators)
    tb_new = tb.loc[mask]
    # Fill None values in `sex`
    # _debug_highlight_none(tb_new, "prevalence_male_circumcision", "sex")
    tb_new = safe_replace_NAs(
        tb=tb_new,
        set_map={
            "prevalence_male_circumcision": {"FORMAL_HEALTHCARE", "TRADITIONAL"},
        },
        dimension="age",
        value="total",
    )
    tb_new = safe_replace_NAs(
        tb=tb_new,
        set_map={
            "prevalence_male_circumcision": {"15_19", "15_49", "20_24", "25_49"},
        },
        dimension="group",
        value="total",
    )
    # Checks
    assert tb_new["hepatitis"].isna().all()
    assert tb_new["estimate"].isna().all()
    assert tb_new["group"].notna().all()
    assert tb_new["age"].notna().all()
    assert set(tb_new["sex"].unique()) == {None, "total"}
    assert tb_new["dimension_0"].isna().all()
    # Drop columns
    tb_new = tb_new.drop(columns=["sex", "hepatitis", "estimate"])

    return tb_new, mask


def extract_no_dim_table_gam(tb):
    """Generate table with only indicators that have no dimension information."""
    indicators_no_dim = [
        "denied_services_hiv_status",
        "expenditure_tb_hiv_activities",
        "hiv_care",
        "hiv_tb_diagnosis",
        "hiv_tb_patients_receiving_art",
        "hiv_tb_patients_receiving_cpt",
        "hiv_tb_patients_under_ipt",
        "new_relapse_tb_cases",
        "newly_enrolled_care",
        "notified_tb_cases",
        "num_art_complt_tpt",
        "num_art_init_tpt",
        "people_art_elig_tpt",
        "people_art_elig_tpt_strt",
        "people_new_art_elig_tpt_strt",
        "people_on_prep",
        "per_tpt_art_cmplt",
        "plhiv_current_enrolled",
        "plhiv_current_tb_preventative",
        "plhiv_current_treatment_tb",
        "plhiv_in_care_with_active_tb",
        "plhiv_receiving_tb_preventive_therapy",
        "prisoners_ost",
        "prisoners_tb",
        "prisoners_condoms_distributed",
        "prop_art_tpt_start",
        "prop_new_art_tpt_strt",
        "pwid_needles",
        "pwid_needles_distributed",
        "resource_needs_ft",
        "tb_expenditure",
        "tb_patients_documented_hiv_positive_status",
        "tb_patients_documented_hiv_status",
        "tb_patients_tested_positive_hiv",
    ]
    mask = tb["indicator"].isin(indicators_no_dim)
    tb_new = tb.loc[mask]
    # Checks
    assert (tb_new["sex"].unique() == "total").all()
    assert (tb_new["age"].unique() == "total").all()
    assert (tb_new["group"].unique() == "total").all()
    assert (tb_new["sex"].unique() == "total").all()
    assert tb_new["dimension_0"].isna().all()

    # Drop columns
    tb_new = tb_new.drop(columns=["sex", "age", "group", "hepatitis", "estimate"])

    return tb_new, mask


def extract_sex_group_table_gam(tb):
    """Generate table with indicators that have no age information."""
    indicators = {
        "condoms_distributed",
        "experience_violence",
        "syphilis_prevalence",
    }
    mask = tb["indicator"].isin(indicators)
    tb_new = tb.loc[mask]
    # Checks
    assert set(tb_new["group"]) == {
        "NGO",
        "men who have sex with men",
        "people who inject drugs",
        "private",
        "public",
        "sex workers",
        "total",
        "transgender, sex workers",
    }
    assert set(tb_new["sex"]) == {
        "female",
        "male",
        "other",
        "total",
    }
    assert set(tb_new["age"].unique()) == {"total"}

    # Drop columns
    tb_new = tb_new.drop(columns=["age"])

    return tb_new, mask


def incorporate_dimension_0(tb):
    """There is dimension information in both `group` and `dimension_0` columns.

    This function brings the dimensional information from `dimension_0` into `group`. To do so, it looks at various cases: when `group` is empty but `dimension_0` isn't, when `group` is "total" and `dimension_0` isn't, when `dimension_0` and `group` are both filled, etc.
    """
    # Incorporate `dimension_0` into `group`
    ## Fill NaNs in `group` with `dimension_0`
    mask = (tb["group"].isna()) & (tb["dimension_0"].notna())
    tb["group"] = tb["group"].fillna(tb["dimension_0"])
    tb.loc[mask, "dimension_0"] = np.nan
    ## Replace 'total' in `group` with `dimension_0`
    mask = (tb["group"] == "total") & (tb["dimension_0"].notna())
    tb.loc[mask, "group"] = tb.loc[mask, "dimension_0"]
    tb.loc[mask, "dimension_0"] = np.nan

    # When `dimension_0` is "transgender" and `group` is "trans*"
    ## Solution: I just use the value from `group` and drop `dimension_0`
    mask = (tb["dimension_0"] == "transgender") & (tb["group"].str.startswith("trans"))
    assert set(tb.loc[mask, "indicator"].unique()) == {
        "art_coverage",
        "condom_use",
        "hiv_prevalence",
        "hiv_programmes_coverage",
        "hiv_status_awareness",
    }
    tb.loc[mask, "dimension_0"] = np.nan

    # When `dimension_0` is not NA and `group` is "transgender"
    ## Solution: Combine `group` and `dimension_0` with a comma.
    mask = (tb["group"].notna()) & (tb["dimension_0"].notna())
    assert set(tb.loc[mask, "group"]) == {"transgender"}
    assert set(tb.loc[mask, "dimension_0"]) == {"sex workers", "prisoners", "people who inject drugs"}
    tb.loc[mask, "group"] = tb.loc[mask, "group"] + ", " + tb.loc[mask, "dimension_0"]
    tb.loc[mask, "dimension_0"] = np.nan

    # Final checks
    assert not ((tb.group.isna()) & (tb.dimension_0.notna())).any()
    assert not ((tb.group.notna()) & (tb.dimension_0.notna())).any()

    # DEBUG
    # # 1) Nothing in group, nothing in dimension_0 => NO ACTION
    # tb_1 = tb[(tb.group.isna()) & (tb.dimension_0.isna())]
    # print("1)", len(tb_1), "NO ACTION: nothing in group, nothing in dimension_0")
    # # 2) Something in group, nothing in dimension_0 => NO ACTION
    # tb_2 = tb[(tb.group.notna()) & (tb.dimension_0.isna())]
    # print("2)", len(tb_2), "NO ACTION: something in group, nothing in dimension_0")
    # # 3) Nothing in group, something in dimension_0 => EASY REPLACE
    # tb_3 = tb[(tb.group.isna()) & (tb.dimension_0.notna())]
    # print("3)", len(tb_3), "ACTION REQUIRED: nothing in group, something in dimension_0")
    # # 4) Something in group, something in dimension_0 => HARD REPLACE
    # tb_4 = tb[(tb.group.notna()) & (tb.dimension_0.notna())]
    # print("4)", len(tb_4), "ACTION REQUIRED: something in group, something in dimension_0")
    # # 5) Optional
    # tb.loc[(tb.group.notna()) & (tb.dimension_0.notna()), ["dimension_0", "group"]].drop_duplicates()

    # Drop dimension_0
    tb = tb.drop(columns=["dimension_0"])
    return tb


def handle_nulls_in_dimensions_gam(tb):
    """Make sure that there is no None/NA values in `sex`, `age`, `group` dimensions.

    Instead, we should use "total" when the value is not available.
    """
    # 1/ Fill `sex`
    # _debug_highlight_none(tb, "art_coverage", "sex")
    ## keys: Indicators to set sex -> "total". values: dimensions expected to be None/NA
    set_sex_to_total = {
        "art_coverage": {
            "LESS_25",
            "25_AND_UP",
        },
        "avoidance_care": {
            "LESS_25",
            "25_AND_UP",
        },
        "condom_use": {
            "LESS_25",
            "25_AND_UP",
        },
        "experience_stigma": {
            "LESS_25",
            "25_AND_UP",
        },
        "hiv_prevalence": {
            "LESS_25",
            "25_AND_UP",
        },
        "hiv_programmes_coverage": {
            "LESS_25",
            "25_AND_UP",
        },
        "hiv_status_awareness": {
            "LESS_25",
            "25_AND_UP",
        },
        "pwid_safety": {
            "LESS_25",
            "25_AND_UP",
        },
    }

    tb = safe_replace_NAs(tb, set_sex_to_total, "sex", "total")

    # 2/ Fill `age`
    # _debug_highlight_none(tb, "syphilis_prevalence", "age")
    set_age_to_total = {
        "art_coverage": {
            "TRANSGENDER",
            "TRANSMAN",
            "TRANSWOMAN",
        },
        "avoidance_care": {
            "TRANSGENDER",
        },
        "condom_use": {
            "TRANSGENDER",
            "TRANSMAN",
            "TRANSWOMAN",
        },
        "hiv_prevalence": {
            "TRANSGENDER",
            "TRANSMAN",
            "TRANSWOMAN",
        },
        #
        "hiv_programmes_coverage": {
            "TRANSGENDER",
            "TRANSMAN",
            "TRANSWOMAN",
            "TRANSOTHER",
        },
        "hiv_status_awareness": {
            "TRANSGENDER",
            "TRANSMAN",
            "TRANSWOMAN",
        },
        "pwid_safety": {
            "TRANSGENDER",
        },
        "syphilis_prevalence": {
            "TRANSGENDER",
            "ALL_SEXES",
        },
    }

    tb = safe_replace_NAs(tb, set_age_to_total, "age", "total")

    # 4/ Fill `group`
    # _debug_highlight_none(tb, "pwid_safety", "group")
    # sorted(tb.loc[tb.group.isna(), "indicator"].unique())
    set_group_to_total = {
        "hiv_tests": {
            "0_14",
            "FEMALE_ADULTS",
            "MALE_ADULTS",
        },
        "pwid_safety": {
            "LESS_25",
            "25_AND_UP",
            "FEMALES",
            "MALES",
        },
    }

    tb = safe_replace_NAs(tb, set_group_to_total, "group", "total")

    #  5/ Check that there is no NA/None rows
    assert not tb.isna().any().any(), "Some NaNs were detected, but none were expected!"

    return tb


def expand_raw_dimension(tb, dimensions, dimension_names=None, drop=True):
    """Add sex, age, estimate dimensions.

    sex: female, male, total
    age: age group range
    estimate: estimate, lower, upper
    """
    if dimension_names is None:
        dimension_names = ["sex", "age", "estimate"]
    # Add new dimension columns
    dim_values = {dim: {k: v.get(dim) for k, v in dimensions.items()} for dim in dimension_names}
    for dim in dimension_names:
        tb[dim] = tb["dimension"].map(dim_values[dim])

    # Drop old dimensions column
    if drop:
        tb = tb.drop(columns=["dimension", "dimension_name"])

    return tb


def _debug_highlight_none(tb, indicator_name, column):
    """Show indicator-age-sex-group combinations, highlighting rows where `column` is None.

    Typical use:

    for indicator_name in indicator_names:
        print(indicator_name)
        print("====================")
        _debug_highlight_none(tb, indicator_name, "group")
    """

    def highlight_row(row, condition):
        return ["background-color: yellow; color: black" if condition[row.name] else "" for _ in row]

    cols = ["age", "sex", "group"]
    x = (
        tb.loc[tb["indicator"] == indicator_name, ["indicator"] + cols + ["dimension", "indicator_description", "unit"]]
        .drop_duplicates()
        .sort_values(cols)
    )
    condition = tb[column].isna()
    x = x.style.apply(highlight_row, condition=condition, axis=1)
    # display(x)


def safe_replace_NAs(tb, set_map, dimension, value):
    """Replace NAs/None in tb[dimension] with value.

    This function only applies to rows with indicators specified by set_map. It checks that the None/NA values are expected (based on value from column `dimension`).
    """
    mask = tb["indicator"].isin(set_map.keys())
    tbx = tb.loc[mask, ["indicator", dimension, "dimension"]].drop_duplicates()
    tmp = tbx.loc[tbx[dimension].isna()].groupby("indicator").dimension.apply(set).to_dict()
    if tmp != set_map:
        expected_not_found = set(set_map) - set(tmp)
        unexpected_found = set(tmp) - set(set_map)
        raise ValueError(
            f"Expected and found values do not match!\nexpected (not found): {expected_not_found}\nfound (not expected): {unexpected_found}"
        )
    mask = tb["indicator"].isin(set_map.keys())
    tb.loc[mask, dimension] = tb.loc[mask, dimension].fillna(value)

    return tb


def add_regional_aggregates(tb: Table, ds_regions, ds_income_groups, columns_agg, countries_all) -> Table:
    """
    Adding regional aggregates for all tuberculosis variables.
    """
    COLUMNS_INDEX_BASE = [
        "country",
        "year",
        "sex",
        "age",
        "group",
        "hepatitis",
        "estimate",
    ]
    index_columns = list(tb.columns.intersection(COLUMNS_INDEX_BASE))

    # Split table into 'agg' and 'no_agg'
    tb_agg = tb[index_columns + columns_agg]
    tb_no_agg = tb.drop(columns=columns_agg)

    # Removing existing aggregates
    tb_agg = tb_agg[~tb_agg["country"].isin(REGIONS_TO_ADD)]
    tb_agg = tb_agg.dropna(subset=columns_agg, how="all")

    # Add NAs
    countries_all = pd.Index(countries_all, dtype="string")
    cols = ["country", *[col for col in index_columns if col != "country"]]
    index = pd.MultiIndex.from_product(
        [countries_all, *[tb[col].unique() for col in cols[1:]]],
        names=cols,
    )
    tb = tb.set_index(cols).reindex(index).reset_index()

    # Create a table for each disaggregation value
    # Add region aggregates.
    tb_agg = geo.add_regions_to_table(
        tb=tb_agg,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        regions=REGIONS_TO_ADD,
        index_columns=index_columns,
        # min_num_values_per_year=1,
        frac_allowed_nans_per_year=0.3,
        countries_that_must_have_data={
            "Asia": ["China", "India"],
            "North America": ["United States", "Canada", "Mexico"],
            "Europe": ["Germany", "France", "United Kingdom", "Italy", "Spain"],
            "Africa": ["Nigeria", "Ethiopia", "Kenya"],
            "South America": ["Brazil", "Argentina", "Colombia"],
            "Oceania": ["Australia", "New Zealand"],
        },
    )

    # Drop unnecessary rows
    tb_agg = tb_agg.dropna(subset=columns_agg, how="all")

    # Merge with table without aggregates
    tb = pr.merge(tb_no_agg, tb_agg, on=index_columns, how="outer").reset_index(drop=True)

    return tb


##########################################################################################
# OTHERS (might deprecate)
##########################################################################################


def add_old_art_averted_deaths_data(tb: Table) -> Table:
    """Complement data with tables shared by UNAIDS via mail correspondance."""
    # Read ART prevented deaths table
    tb_aux = load_aux_table("unaids_deaths_averted_art")

    # Checks
    metric = "deaths_averted_art"
    assert metric in tb_aux.columns
    assert metric in tb.columns

    # Minor formatting
    tb_aux = tb_aux.rename(columns={"subgroup_description": "estimate"})
    tb_aux["sex"] = "total"
    tb_aux["age"] = "total"
    estimates_renaming = {
        "estimate": "central",
        "upper estimate": "high",
        "lower estimate": "low",
    }
    assert set(tb_aux["estimate"].unique()) == set(estimates_renaming)
    tb_aux["estimate"] = tb_aux["estimate"].replace(estimates_renaming)

    # Combine tables
    tb = tb.merge(tb_aux, on=["country", "year", "estimate", "sex", "age"], how="outer", suffixes=("", "__aux"))
    tb[metric] = tb[metric].fillna(pd.Series(tb[f"{metric}__aux"]))

    # Drop auxiliary columns
    tb = tb.drop(columns=[f"{metric}__aux"])

    return tb


def load_aux_table(short_name: str) -> Table:
    """Load auxiliary table.

    An auxiliary table is a table coming from a dataset that was not sourced from the official API.
    """
    # Load dataset
    ds = paths.load_dataset(short_name)
    # Read table
    tb = ds.read(short_name)

    # Harmonize country names
    log.info(f"health.unaids: harmonize countries ({short_name})")
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    return tb


def combine_tables(tb: Table, tb_hiv_child: Table, tb_gap_art: Table, tb_deaths_art: Table, tb_condom: Table) -> Table:
    """Combine all tables."""
    tb = pr.concat([tb, tb_hiv_child], ignore_index=True)

    # Add remaining data from auxiliary tables

    # Indicator names and their corresponding auxiliary tables
    indicators = ["msm_condom_use", "deaths_averted_art", "gap_on_art"]
    tables = [tb_condom, tb_deaths_art, tb_gap_art]
    for metric, tb_aux in zip(indicators, tables):
        tb = tb.merge(tb_aux, on=["country", "year", "subgroup_description"], how="outer", suffixes=("", "__aux"))
        tb[metric] = tb[metric].fillna(pd.Series(tb[f"{metric}__aux"]))

    # Drop auxiliary columns
    tb = tb.drop(columns=["msm_condom_use__aux", "deaths_averted_art__aux", "gap_on_art__aux"])

    return tb


def get_all_countries(ds_regions, ds_income_groups):
    countries_all = []
    for region in REGIONS_TO_ADD:
        members = geo.list_members_of_region(
            region=region,
            ds_regions=ds_regions,
            ds_income_groups=ds_income_groups,
            exclude_historical_countries=True,
        )
        countries_all.extend(members)
    countries_all = set(countries_all)
    return list(countries_all)
