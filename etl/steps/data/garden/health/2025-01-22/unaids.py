"""Processing of UNAIDS data.

There are two main UNAIDS sources: EPI, GAM, KPA, NCPI. More details in the snapshot step.
"""

from typing import cast

import numpy as np
import owid.catalog.processing as pr
import pandas as pd
import yaml
from owid.catalog import Dataset, Table
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
    "World",
]

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
    # ## Load regions dataset.
    # ds_regions = paths.load_dependency("regions")
    # ## Load income groups dataset.
    # ds_income_groups = paths.load_dependency("income_groups")

    # Load dimensions
    with paths.side_file("unaids.dimensions.yml").open() as f:
        dimensions = yaml.safe_load(f)

    with paths.side_file("unaids.indicators_to_dimensions.yml").open() as f:
        dimensions_collapse = yaml.safe_load(f)

    ###############################
    # EPI
    ###############################
    tb_epi = ds_meadow.read("epi")  ## 1,687,587 rows
    tb_epi = make_table_epi(tb_epi, dimensions["epi"])
    tb_epi = tb_epi.format(["country", "year", "age", "sex", "estimate"], short_name="epi")

    ###############################
    # GAM
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
    tb_only_estimate, mask_only_estimate = extract_estimate_table_gam(tb)
    # 2.2/ [group]. Separate data with no sex and no age.
    tb_only_group, mask_only_group = extract_only_group_table_gam(tb)
    # 2.3/ [age]. Separate data with no sex and group.
    tb_only_age, mask_only_age = extract_only_age_table_gam(tb)

    # 2.4/ [sex]. Separate data with no age and group.
    tb_only_sex, mask_only_sex = extract_only_sex_table_gam(tb)

    # 3.1/ [age, sex]. Separate data with NO group data.
    tb_no_group, mask_no_group = extract_no_group_table_gam(tb)
    # 3.2/ [age, group]. Separate data with NO sex.
    tb_no_sex, mask_no_sex = extract_no_sex_table_gam(tb)

    # 4/ []. Separate data with NO dimension.
    tb_no_dim, mask_no_dim = extract_no_dim_table_gam(tb)

    # 5/ Drop separated data from main table
    tb = tb.loc[
        ~(
            mask_hepatitis
            | mask_only_estimate
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
    tb_no_age, mask_no_age = extract_no_age_table_gam(tb)
    ## Correction
    mask = (tb_no_age.country == "China") & (tb_no_age.year == 2021)
    x = tb_no_age.loc[mask]
    assert len(x) == 2
    assert set(x["value"].unique()) == {4.1}
    assert set(x["dimension"].unique()) == {"TOTAL", "ALL_AGES"}
    tb_no_age = tb_no_age.loc[
        ~((tb_no_age["country"] == "China") & (tb_no_age["year"] == 2021) & (tb_no_age["dimension"] == "TOTAL"))
    ]
    # Remove from main table!
    tb = tb.loc[~mask_no_age]

    # 4/ Checks
    paths.log.info("GAM: Checking dimensions")
    _ = tb.format(["country", "year", "indicator", "group", "sex", "age"])
    _ = tb_hepatitis.format(["country", "year", "indicator", "group", "sex", "age", "hepatitis"])
    _ = tb_only_estimate.format(["country", "year", "indicator", "estimate"])
    _ = tb_only_group.format(["country", "year", "indicator", "group"])
    _ = tb_only_age.format(["country", "year", "indicator", "age"])
    _ = tb_only_sex.format(["country", "year", "indicator", "sex"])  # TODO: review
    _ = tb_no_group.format(["country", "year", "indicator", "sex", "age"])
    _ = tb_no_sex.format(["country", "year", "indicator", "group", "group", "age"])
    _ = tb_no_age.format(["country", "year", "indicator", "group", "sex"])
    _ = tb_no_dim.format(["country", "year", "indicator"])

    # RESHAPE
    def pivot_and_format(tb, columns, short_name):
        # 1/ Save some fields (this might be useful later)
        tb_meta = tb[["indicator", "indicator_description", "unit"]].drop_duplicates()
        assert (
            not tb_meta[["indicator", "indicator_description"]].duplicated().any()
        ), "Multiple descriptions or units for a single indicator!"

        # 2/ Pivot
        tb = tb.pivot(index=columns, columns="indicator", values="value").reset_index()
        tb = tb.underscore()
        tb = tb.dropna(how="all")

        tb = tb.format(columns, short_name=short_name)

        return tb

    # TABLE GROUPS
    paths.log.info("GAM: pivoting and formatting")
    tables_gam = [
        pivot_and_format(tb, ["country", "year", "age", "sex", "group"], "gam_age_sex_group"),
        pivot_and_format(tb_hepatitis, ["country", "year", "age", "sex", "group", "hepatitis"], "gam_hepatitis"),
        pivot_and_format(tb_only_estimate, ["country", "year", "estimate"], "gam_estimates"),
        pivot_and_format(tb_only_group, ["country", "year", "group"], "gam_group"),
        pivot_and_format(tb_only_age, ["country", "year", "age"], "gam_age"),
        pivot_and_format(tb_only_sex, ["country", "year", "sex"], "gam_sex"),
        pivot_and_format(tb_no_group, ["country", "year", "age", "sex"], "gam_age_sex"),
        pivot_and_format(tb_no_sex, ["country", "year", "age", "group"], "gam_age_group"),
        pivot_and_format(tb_no_age, ["country", "year", "sex", "group"], "gam_sex_group"),
        pivot_and_format(tb_no_dim, ["country", "year"], "gam"),
    ]

    tbx = tb_no_dim
    tb_meta = tbx[["indicator", "indicator_description", "unit"]].drop_duplicates()
    for _, row in tb_meta.iterrows():
        print(
            f"{row.indicator}:\n\ttitle: {row.indicator_description}\n\tunit: {row['unit']}\n\tdescription_short: ''\n\tdescription_from_producer: ''"
        )

    ####################
    # tbx = tb.groupby("indicator", as_index=False).agg(
    #     {
    #         "age": ("unique", "nunique"),
    #         "sex": ("unique", "nunique"),
    #         "group": ("unique", "nunique"),
    #     }
    # )
    # tbx.columns = [f"{col[0]}_{col[1]}" if col[1] != "" else col[0] for col in tbx.columns]
    # tbx = tbx.sort_values(["age_nunique", "sex_nunique", "group_nunique"])
    # tbx

    # tbx[(tbx.age_nunique == 1) & (tbx.sex_nunique == 1) & (tbx.group_nunique == 1)]
    # sorted(tbx.loc[(tbx["age_nunique"] == 1) & (tbx["sex_nunique"] == 1) & (tbx["group_nunique"] == 1), "indicator"])

    #
    # Process data.
    #
    # log.info("health.unaids: handle NaNs")
    # tb = handle_nans(tb)

    # # Harmonize country names (main table)
    # log.info("health.unaids: harmonize countries (main table)")
    # tb = geo.harmonize_countries(
    #     df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    # )

    # # Pivot table
    # log.info("health.unaids: pivot table")
    # tb = tb.pivot(
    #     index=["country", "year", "subgroup_description"], columns="indicator", values="obs_value"
    # ).reset_index()

    # # Underscore column names
    # log.info("health.unaids: underscore column names")
    # tb = tb.underscore()

    # Scale indicators
    # indicators = [
    #     "resource_avail_constant",
    #     "resource_needs_ft",
    # ]
    # scale_factor = 1e6
    # tb[indicators] *= scale_factor
    # # Complement table with auxiliary data
    # log.info("health.unaids: complement with auxiliary data")
    # tb = complement_with_auxiliary_data(tb)

    # Add per_capita
    # log.info("health.unaids: add per_capita")
    # tb = add_per_capita_variables(tb, ds_population)

    # Add region aggregates for TB variables
    # log.info("health.unaids: add region aggregates for TB variables")
    # tb = add_regions_to_tuberculosis_vars(tb, ds_regions, ds_income_groups)
    # Set index
    # log.info("health.unaids: set index")
    # tb = tb.set_index(["country", "year", "disaggregation"], verify_integrity=True)

    # Drop all NaN rows
    # tb = tb.dropna(how="all")

    # Set table's short_name
    # tb.metadata.short_name = paths.short_name

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


def make_table_epi(tb, dimensions):
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

    # Estimate regional data (only when unit == NUMBER)
    # TODO

    # Drop all NaN rows
    tb = tb.dropna(how="all")

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
    tb.loc[tb["dimension_0"] == "men who have sex with men", "sex"] = "male"

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


def extract_only_group_table_gam(tb):
    """Separate indicators with data without sex or age (only group)."""
    indicators_only_group = [
        "discrimination_hc_settings",  # only group
        "domestic_spending_fund_source",  # only group
        "resource_avail_constant",  # only group
    ]
    mask_only_group = tb["indicator"].isin(indicators_only_group)
    tb_new = tb.loc[mask_only_group]
    # Checks
    assert set(tb_new["sex"].unique()) == {None, "total"}
    assert set(tb_new["age"].unique()) == {None, "total"}
    assert tb_new["hepatitis"].isna().all()
    assert tb_new["estimate"].isna().all()
    assert tb_new["group"].notna().all()
    assert set(tb_new.loc[tb_new["group"] == "total", "dimension"].unique()) == {"TOTAL"}
    assert tb_new["dimension_0"].isna().all()
    # Drop columns
    tb_new = tb_new.drop(columns=["sex", "age", "hepatitis", "estimate"])

    return tb_new, mask_only_group


def extract_only_age_table_gam(tb):
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


def extract_only_sex_table_gam(tb):
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


def extract_no_group_table_gam(tb):
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
    # _debug_highlight_none(tb_no_group, "att_tow_wife_beating", "sex")
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


def extract_no_sex_table_gam(tb):
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


def extract_no_age_table_gam(tb):
    """Generate table with indicators that have no age information."""
    indicators = {
        "condoms_distributed_pp",
        "condoms_distributed",
        "experience_violence",
        "population",
        "syphilis_prevalence",
    }
    mask = tb["indicator"].isin(indicators)
    tb_new = tb.loc[mask]
    # Checks
    assert set(tb_new["group"]) == {
        "NGO",
        "men who have sex with men",
        "people who inject drugs",
        "prisoners",
        "private",
        "public",
        "sex workers",
        "total",
        "transgender",
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

    # 3/ Fill `age`
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
        # "prevalence_male_circumcision": {
        #     "FORMAL_HEALTHCARE",
        #     "TRADITIONAL",
        # },
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
        # "prevalence_male_circumcision": {
        #     "15_19",
        #     "15_49",
        #     "20_24",
        #     "25_49",
        # },
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


##########################################################################################
# OTHERS (might deprecate)
##########################################################################################


def add_regions_to_tuberculosis_vars(tb: Table, ds_regions: Dataset, ds_income_groups: Dataset) -> Table:
    """
    Adding regional aggregates for all tuberculosis variables.
    """
    cols = ["hiv_tb_patients_receiving_art", "tb_patients_tested_positive_hiv", "tb_related_deaths"]

    # Split table into 'agg' and 'no_agg'
    tb_agg = tb[["country", "year", "disaggregation"] + cols]
    tb_no_agg = tb.drop(columns=cols)
    # Removing existing aggregates
    tb_agg = tb_agg[~tb_agg["country"].isin(REGIONS_TO_ADD)]
    tb_agg = tb_agg.dropna(subset=cols, how="all")
    # Group tb_agg by disaggregation to allow us to add regions for each disaggregation value
    tb_agg_groups = tb_agg.groupby("disaggregation")

    # Create a table for each disaggregation value
    tbs = []
    for group_name, tb_agg_group in tb_agg_groups:
        tb_agg_group_with_regions = geo.add_regions_to_table(
            tb_agg_group.drop(columns="disaggregation"),
            ds_regions,
            ds_income_groups,
            REGIONS_TO_ADD,
            min_num_values_per_year=1,
            frac_allowed_nans_per_year=0.3,
        )
        tb_agg_group_with_regions["disaggregation"] = group_name
        tbs.append(tb_agg_group_with_regions)

    # Combine all 'agg' tables
    tb_agg = pr.concat(tbs, ignore_index=True)
    # Merge with table without aggregates
    tb = pr.merge(tb_no_agg, tb_agg, on=["country", "year", "disaggregation"], how="outer").reset_index(drop=True)

    return tb


def handle_nans(tb: Table) -> Table:
    """Handle NaNs in the dataset.

    - Replace '...' with NaN
    - Ensure no NaNs for non-textual data
    - Drop NaNs & check that all textual data has been removed
    """
    # Replace '...' with NaN
    tb["obs_value"] = tb["obs_value"].replace("...", np.nan)
    # Ensure no NaNs for non-textual data
    assert not tb.loc[-tb["is_textualdata"], "obs_value"].isna().any(), "NaN values detected for not textual data"
    # Drop NaNs & check that all textual data has been removed
    tb = tb.dropna(subset="obs_value")
    assert tb["is_textualdata"].sum() == 0, "NaN"

    return tb


def add_per_capita_variables(tb: Table, ds_population: Dataset) -> Table:
    """Add per-capita variables.

    Parameters
    ----------
    tb : Table
        Primary data.
    ds_population : Dataset
        Population dataset.

    Returns
    -------
    tb : Table
        Data after adding per-capita variables.

    """
    tb = tb.copy()

    # Estimate per-capita variables.
    ## Only consider variable "domestic_spending_fund_source"
    mask = tb["domestic_spending_fund_source"].isna()

    ## Add population variable
    tb_fund = geo.add_population_to_table(tb[~mask], ds_population, expected_countries_without_population=[])

    ## Estimate ratio
    tb_fund["domestic_spending_fund_source_per_capita"] = (
        tb_fund["domestic_spending_fund_source"] / tb_fund["population"]
    )

    ## Combine tables again
    tb = pr.concat([tb_fund, tb[mask]], ignore_index=True)

    # Drop unnecessary column.
    tb = tb.drop(columns=["population"])

    return tb


def complement_with_auxiliary_data(tb: Table) -> Table:
    """Complement data with tables shared by UNAIDS via mail correspondance."""
    # Load auxiliary tables
    log.info("health.unaids: load auxiliary table with HIV prevalence estimates for children (0-14)")
    tb_hiv_child = load_aux_table("unaids_hiv_children")

    log.info("health.unaids: load auxiliary table with gap to target ART coverage (old years)")
    tb_gap_art = load_aux_table("unaids_gap_art")

    log.info("health.unaids: Load auxiliary table with condom usage among men that have sex with men (old years)")
    tb_condom = load_aux_table("unaids_condom_msm")

    log.info("health.unaids: Load auxiliary table with deaths averted due to ART coverage (old years)")
    tb_deaths_art = load_aux_table("unaids_deaths_averted_art")

    # Combine tables
    log.info("health.unaids: combine main table with auxiliary tables")
    tb = combine_tables(tb, tb_hiv_child, tb_gap_art, tb_deaths_art, tb_condom)

    return tb


def load_aux_table(short_name: str) -> Table:
    """Load auxiliary table.

    An auxiliary table is a table coming from a dataset that was not sourced from the official API.
    """
    # Load dataset
    ds = cast(Dataset, paths.load_dependency(short_name))
    # Etract table
    tb = ds[short_name].reset_index()

    # Harmonize country names
    log.info(f"health.unaids: harmonize countries ({short_name})")
    tb: Table = geo.harmonize_countries(
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
