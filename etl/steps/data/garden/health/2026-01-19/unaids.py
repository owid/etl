"""UNAIDS Garden ETL Step

This script processes UNAIDS HIV/AIDS data from meadow to garden stage, handling
epidemiological estimates (EPI) and Global AIDS Monitoring (GAM) data.

## Harmonization Pipeline

The three YAML files are applied sequentially to transform raw data:

    ┌───────────────────────────────────────────────────────────────────────────────┐
    │  RAW DATA                                                                     │
    │  indicator_id: "HIV_PREVALENCE", dimension_id: "FEMALES_15_24_ESTIMATE"       │
    └───────────────────────────────┬───────────────────────────────────────────────┘
                                    │
                                    ▼
    ┌───────────────────────────────────────────────────────────────────────────────┐
    │  1. indicator_renames.yml                                                     │
    │     Rename indicator IDs → short names (or drop if null)                      │
    │     "HIV_PREVALENCE" → "hiv_prevalence"                                       │
    └───────────────────────────────┬───────────────────────────────────────────────┘
                                    │
                                    ▼
    ┌───────────────────────────────────────────────────────────────────────────────┐
    │  2. dimensions.yml                                                            │
    │     Parse dimension IDs → structured columns                                  │
    │     "FEMALES_15_24_ESTIMATE" → {sex: female, age: 15-24, estimate: central}   │
    └───────────────────────────────┬───────────────────────────────────────────────┘
                                    │
                                    ▼
    ┌───────────────────────────────────────────────────────────────────────────────┐
    │  3. indicators_to_dimensions.yml  (GAM only)                                  │
    │     Consolidate related indicators → one indicator + group dimension          │
    │     tg_hiv_prevalence, msm_hiv_prevalence, ... → hiv_prevalence + group       │
    └───────────────────────────────┬───────────────────────────────────────────────┘
                                    │
                                    ▼
    ┌───────────────────────────────────────────────────────────────────────────────┐
    │  FINAL DATA                                                                   │
    │  indicator: "hiv_prevalence", sex: "female", age: "15-24", group: "..."       │
    └───────────────────────────────────────────────────────────────────────────────┘

## YAML File Details

### 1. unaids.indicator_renames.yml
Maps raw indicator IDs to clean short names. Set to `null` to drop:

    "PLWH": "plwh"          # kept
    "TARGET_IPR": null      # dropped

### 2. unaids.dimensions.yml
Maps dimension IDs to structured values (sex, age, estimate, group, hepatitis):

    "FEMALES_15_24_HIGH_ESTIMATE":
        sex: female
        age: 15-24
        estimate: high

### 3. unaids.indicators_to_dimensions.yml
Collapses multiple indicators into one + dimension (GAM data only):

    - name: hiv_prevalence
      indicators_origin:
        tg_hiv_prevalence:    {dimension: transgender}
        msm_hiv_prevalence:   {dimension: men who have sex with men}
        pwid_hiv_prevalence:  {dimension: people who inject drugs}
"""

import numpy as np
import owid.catalog.processing as pr
import pandas as pd
import yaml

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


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
DIMENSION_COLUMNS = ["sex", "age", "estimate"]
with paths.side_file("unaids.indicator_renames.yml").open() as f:
    INDICATOR_SHORT_NAME_MAPPING = yaml.safe_load(f)


"""
TODO:
- Review NCPI: there are too few data points
- Normalize names for GAM
- Explore potential useful indicators in KPA/NCPI (check for year-country coverage)
    tb_kpa["country_year"] = tb_kpa["country"] + (tb_kpa["year"]).astype("string")
    results = tb_kpa.groupby("indicator").agg({
        "year": "nunique",
        "country": "nunique",
        "country_year": "nunique"
    })
    results.sort_values("country_year", ascending=False).head(30)

    KPA: Seems interesting, but convoluted: it includes data pre 2020 and non-country data (regional).
    NCPI: unclear
- Normalize dimensions


- Reach out to UNAIDS:
    - Indicators missing in indicator registry
    - Indicators in GAM and NCPI
    - Check reliability of indicator (see s&r health)

"""

# ANOMALIES
ANOMALIES = {
    "epi": [
        {
            "indicator": "aids_orphans",
            "country": "Uzbekistan",
        }
    ],
    "gam_group": [
        {
            "indicator": "resource_avail_constant",
            "country": "World",
            "dimensions": {"group": "other international"},
            "year": 2015,
        }
    ],
    # Example
    # "table_name": [
    #     {
    #         "indicator": "indicator_name",
    #         "country": "country_name",
    #         # OPTIONAL
    #         # "year": 2021, OR "year": [2020, 2021] OR "year": list(range(2000, 2022))
    #         # "dimensions": {"age": "0"} OR {"age": ["0", "15-24"]}
    #     }
    # ],
}


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("unaids")
    ds_art_old = paths.load_dataset("unaids_deaths_averted_art")

    # Load auxiliary table
    tb_art_old = ds_art_old.read("unaids_deaths_averted_art")

    # Load dimensions
    with paths.side_file("unaids.dimensions.yml").open() as f:
        dimensions = yaml.safe_load(f)

    with paths.side_file("unaids.indicators_to_dimensions.yml").open() as f:
        dimensions_collapse = yaml.safe_load(f)

    ###############################
    # EPI data
    ###############################
    tb_epi = ds_meadow.read("epi")  ## 1,850,098 rows

    # # Create EPI table
    tb_epi = make_table_epi(tb_epi, dimensions, tb_art_old)  # 360,223 rows

    # # Format
    tb_epi = tb_epi.format(["country", "year", "age", "sex", "estimate"], short_name="epi")

    ###############################
    # KPA data
    ###############################
    tb = ds_meadow.read("kpa")  ## 49,971 rows

    ###############################
    # GAM data
    ###############################
    tb = ds_meadow.read("gam")  ## 124,570 rows

    ################
    # SANITY CHECKS
    ################
    tb = clean_indicator_names(tb, "gam")
    # Sanity check dimensions
    _check_dimensions(tb, dimensions, "gam")

    ################
    # CLEANING
    ################
    # Handle countries: Harmonize, drop non-countries, etc.
    tb = handle_countries_gam(tb)

    # Drop non-relevant (or non-supported) indicators
    mask = (tb["indicator"] == "population") & (tb["dimension"] == "Total")
    tb = tb.loc[~mask]

    # Handle dimensions: Expand raw dimensions, group indicators, etc.
    # This creates `dimension_0` (temporary column with group info from indicator consolidation)
    # which gets merged into `group` later via `incorporate_dimension_0()`.
    tb = handle_dimensions_clean_gam(
        tb,
        dimensions,
        dimensions_collapse_gam=dimensions_collapse["gam"],
    )

    tb = tb.astype(
        {
            "indicator": "string",
            "dimension_0": "string",
            "country": "string",
        }
    )

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

    tb, tb_sex_group = extract_tbs(tb)

    # Condoms per capita
    tb_no_dim = add_condoms_per_100k(tb_no_dim, tb_sex_group)

    # RESHAPE (and check)
    paths.log.info("GAM: Format (and check)")
    tb = pivot_and_format(
        tb,
        ["country", "year", "age", "sex", "group"],
        "gam_age_sex_group",
    )
    tb_hepatitis = pivot_and_format(
        tb_hepatitis,
        ["country", "year", "age", "sex", "group", "hepatitis"],
        "gam_hepatitis",
    )
    tb_estimate = pivot_and_format(
        tb_estimate,
        ["country", "year", "estimate"],
        "gam_estimates",
    )
    tb_group = pivot_and_format(tb_group, ["country", "year", "group"], "gam_group")
    tb_age = pivot_and_format(tb_age, ["country", "year", "age"], "gam_age")  # ERR
    tb_sex = pivot_and_format(tb_sex, ["country", "year", "sex"], "gam_sex")
    tb_age_sex = pivot_and_format(tb_age_sex, ["country", "year", "age", "sex"], "gam_age_sex")
    tb_age_group = pivot_and_format(tb_age_group, ["country", "year", "age", "group"], "gam_age_group")
    tb_sex_group = pivot_and_format(tb_sex_group, ["country", "year", "sex", "group"], "gam_sex_group")
    tb_no_dim = pivot_and_format(
        tb_no_dim,
        ["country", "year"],
        "gam",
    )  # ERR

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
        "experience_violence",  #
        "hiv_prevalence",
        "hiv_programmes_coverage",
        "hiv_status_awareness",
        "hiv_tests",
        "pwid_safety",
        "syphilis_prevalence",
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
    # for tb in tables_gam:
    #     cols = tb.columns
    #     for col in cols:
    #         print(col, tb[col].m.origins)

    ###############################
    # EXPORT
    ###############################
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    tables = [
        tb_epi,
        *tables_gam,
    ]
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=tables, default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()


# EPI
def make_table_epi(tb, dimensions, tb_aux):
    ################
    # SANITY CHECKS
    ################
    # Sanity check indicators, keep relevant
    tb = clean_indicator_names(tb, "epi")
    # Sanity check dimensions
    _check_dimensions(tb, dimensions, "epi")
    # Sanity check source
    sources_expected = {
        "Global AIDS Monitoring (GAM) Online Reporting Tool",
        "Routine programme data",
        "UNAIDS epidemiological estimates 2025",
        "UNAIDS_Estimates_",
    }
    assert set(tb["source"].unique()) == sources_expected, "Unexpected source!"
    ## Sanity check value (no NaNs)
    assert not tb["value"].isna().any(), "Some NaNs were detected, but none were expected!"
    # Check unit (fix wrong unit type for mtct_rate)
    assert tb[tb["indicator"] == "mtct_rate"].groupby(["country", "year", "dimension"]).size().max() == 1
    assert set(tb.loc[tb["indicator"] == "mtct_rate", "unit"].unique()) == {"Rate", "Percent"}
    tb.loc[tb["indicator"] == "mtct_rate", "unit"] = "Rate"
    # Save unit info
    tb_units = tb[["indicator", "unit"]].drop_duplicates()
    assert not tb_units["indicator"].duplicated().any(), "Multiple descriptions or units for a single indicator!"

    ################################################################
    # FORMAT TB
    # Harmonize country names, extract dimensions, pivot table
    ################################################################
    # Harmonize
    tb = paths.regions.harmonize_names(tb=tb)

    # Extract dimensions
    tb = expand_raw_dimension(tb, dimensions["epi"], check_na=True)

    # Pivot table (and lower-case columns)
    tb = tb.pivot(index=["country", "year"] + DIMENSION_COLUMNS, columns="indicator", values="value").reset_index()

    ################################################################
    # Process data
    # Old ART data, remove anomalies, region aggregates
    ################################################################
    # Add older ART-prevented deaths data
    tb = add_old_art_averted_deaths_data(tb, tb_aux)

    # Remove anomalies
    if "epi" in ANOMALIES:
        paths.log.info("Removing anomalies from table: 'epi'")
        anomalies = ANOMALIES["epi"]
        tb = remove_anomalies(tb, anomalies)

    # Add regional data
    columns_agg = tb_units.loc[tb_units["unit"] == "Number", "indicator"].tolist()
    if columns_agg != []:
        tb = add_regional_aggregates(
            tb,
            columns_agg=columns_agg,
        )

    return tb


def add_old_art_averted_deaths_data(tb, tb_aux):
    """Complement data with tables shared by UNAIDS via mail correspondance."""
    dtypes = tb.dtypes.to_dict()

    # Process ART prevented deaths table
    tb_aux = paths.regions.harmonize_names(tb=tb_aux)

    # Checks
    metric = "deaths_averted_art"
    assert metric in tb_aux.columns
    assert metric in tb_aux.columns

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

    # Add types
    tb = tb.astype(dtypes)
    return tb


# GAM
def handle_dimensions_clean_gam(tb, dimensions, dimensions_collapse_gam):
    """Process GAM dimensions: consolidate indicators and expand dimension IDs.

    This function applies two transformations that both produce "group" information:

    1. Indicator consolidation (indicators_to_dimensions.yml):
       Multiple indicators → one indicator + `dimension_0` column
       E.g., tg_hiv_prevalence → hiv_prevalence with dimension_0="transgender"

    2. Dimension ID expansion (dimensions.yml):
       Dimension IDs → structured columns including `group`
       E.g., "TRANSGENDER" → group="transgender"

    The `dimension_0` column is a TEMPORARY holding column for group values from
    indicator consolidation. It gets merged into `group` later via `incorporate_dimension_0()`.

    Why two sources? Some group info comes from the indicator name itself (consolidated
    indicators), while other group info comes from the dimension ID string.
    """
    # Collapse original indicators into indicator + dimension
    dix = {}
    for dim in dimensions_collapse_gam:
        _dix = {k: {**v, "name": dim["name"]} for k, v in dim["indicators_origin"].items()}
        dix |= _dix
    tb["dimension_0"] = tb["indicator"].map(lambda x: dix[x]["dimension"] if x in dix else None)
    tb["indicator"] = tb["indicator"].map(lambda x: dix[x]["name"] if x in dix else x)
    ## Lower case indicator names
    # tb["indicator"] = tb["indicator"].str.lower()

    # Expand original dimension into multiple dimensions
    tb = expand_raw_dimension(
        tb,
        dimensions["gam"],
        dimension_names=["sex", "age", "group", "hepatitis", "estimate"],
        drop=False,
    )

    # POPULATION: Fix population data (this is done here to avoid it being separated by `extract_estimate_table_gam`)
    mask = tb["indicator"] == "population"
    assert set(tb.loc[mask, "estimate"].unique()) == {"central"}, "Unexpected estimate!"
    tb.loc[mask, "estimate"] = None

    # SEX: set sex="male" and sex="others" when applicable
    ### Set to "male"
    mask = (tb["dimension_0"] == "men who have sex with men") & ~(
        tb["indicator"].isin(["condoms_distributed_pp", "population"])
    )
    tb.loc[mask, "sex"] = "male"

    ### Set to "other"
    tb.loc[
        (tb["group"].astype("string").str.contains("trans"))
        | (tb["dimension_0"].astype("string").str.contains("trans")),
        "sex",
    ] = "other"

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
    tb = paths.regions.harmonize_names(
        tb=tb,
        make_missing_countries_nan=True,
    )

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
    tb_hepatitis_2 = tb.loc[mask_2].copy()
    tb_hepatitis_2.loc[:, "indicator"] = "viral_hepatitis"
    tb_hepatitis_2.loc[:, "hepatitis"] = "C"
    tb_hepatitis_2.loc[:, "group"] = "prisoners"
    # Combine hepatitis data
    tb_hepatitis = pr.concat([tb_hepatitis, tb_hepatitis_2])

    # Checks
    assert set(tb_hepatitis["sex"].unique()) == {"female", "male", "other", "total"}
    assert set(tb_hepatitis["age"].unique()) == {"0-25", "25+", "total"}
    assert set(tb_hepatitis["hepatitis"].unique()) == {"B", "C"}
    assert set(tb_hepatitis["group"].unique()) == {pd.NA, "prisoners", "transgender"}
    assert tb_hepatitis["group"].notna().sum() == 101, "Unexpected not-NAs"

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
        "comanagement_tb_hiv",
        "hiv_new_tb_cases",
        "incident_tb_cases",
        "tb_related_deaths",
    }, "Unexpected extra indicators"
    mask_estimate = tb["indicator"].isin(indicators_estimate)
    tb_estimate = tb.loc[mask_estimate]

    # Checks
    assert set(tb_estimate["sex"].unique()) == {pd.NA, "total"}
    assert set(tb_estimate["age"].unique()) == {pd.NA, "total"}
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
    assert set(tb_new.loc[mask, "group"].unique()) == {"total"}
    tb_new.loc[mask, "group"] = tb_new.loc[mask, "dimension_0"]
    tb_new.loc[mask, "dimension_0"] = None

    # Fix group for indicator "population"
    mask = tb_new["indicator"] == "population"
    assert tb_new.loc[mask, "dimension_0"].notna().all()
    assert tb_new.loc[mask, "group"].isna().all()
    tb_new.loc[mask, "group"] = tb_new.loc[mask, "dimension_0"]
    tb_new.loc[mask, "dimension_0"] = None

    # Checks
    assert set(tb_new["sex"].unique()) == {pd.NA, "other", "total"}
    assert set(tb_new["age"].unique()) == {pd.NA, "total"}
    assert tb_new["hepatitis"].isna().all()
    assert tb_new["estimate"].isna().all()
    assert tb_new["group"].notna().all()
    assert set(tb_new.loc[tb_new["group"] == "total", "dimension"].unique()) == {"Total"}
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
    # _debug_highlight_none(tb_new, "att_tow_wife_beating", "sex")
    tb_new = safe_replace_NAs(
        tb=tb_new,
        set_map={
            "pwid_ost_coverage": {"< 25", "25+"},
            "att_tow_wife_beating": {"All ages"},
            "hiv_pos_rate": {"Children (0-14)"},
        },
        dimension="sex",
        value="total",
    )
    # Checks
    assert set(tb_new["group"].unique()) == {pd.NA, "total"}
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
            "prevalence_male_circumcision": {"Formal healthcare", "Traditional practitioner"},
        },
        dimension="age",
        value="total",
    )
    tb_new = safe_replace_NAs(
        tb=tb_new,
        set_map={
            "prevalence_male_circumcision": {"15-19", "Adults (15-49)", "20-24", "25-29", "25-49"},
        },
        dimension="group",
        value="total",
    )
    # Checks
    assert tb_new["hepatitis"].isna().all()
    assert tb_new["estimate"].isna().all()
    assert tb_new["group"].notna().all()
    assert tb_new["age"].notna().all()
    assert set(tb_new["sex"].unique()) == {pd.NA, "total"}
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
        "share_hiv_tb_patients_receiving_art",
        "hiv_tb_patients_receiving_cpt",
        "share_hiv_tb_patients_receiving_cpt",
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
        "prisoners_tb",
        "prisoners_condoms_distributed",
        "prop_art_tpt_start",
        "prop_new_art_tpt_strt",
        "pwid_needles",
        "prisoners_needles",
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

    # Fix unit for pwid_needles
    x = tb_new.groupby("indicator")["unit"].nunique()
    assert set(x[x > 1].index) == {"pwid_needles"}
    assert set(tb.loc[tb["indicator"] == "pwid_needles", "unit"].unique()) == {"Rate", "Number"}
    tb_new.loc[(tb_new["indicator"] == "pwid_needles"), "unit"] = "Rate"

    # Drop columns
    tb_new = tb_new.drop(columns=["sex", "age", "group", "hepatitis", "estimate"])

    return tb_new, mask


def extract_sex_group_table_gam(tb):
    """Generate table with indicators that have no age information."""
    indicators = {
        "condoms_distributed",
    }
    mask = tb["indicator"].isin(indicators)
    tb_new = tb.loc[mask]
    # Checks
    assert set(tb_new["group"]) == {
        "NGO",
        "private",
        "public",
        "total",
    }
    assert set(tb_new["sex"]) == {
        "female",
        "male",
    }
    assert set(tb_new["age"].unique()) == {"total"}

    # Drop columns
    tb_new = tb_new.drop(columns=["age"])

    # Add condoms distributed, total
    assert tb_new["indicator"].nunique() == 1
    tb_new_total = tb_new.groupby(["country", "year", "indicator", "group"], as_index=False)[["value"]].sum()
    tb_new_total["sex"] = "total"
    tb_new_total["unit"] = "Number"
    tb_new_total["source"] = "UNAIDS_GAM_"
    tb_new = pr.concat([tb_new, tb_new_total], ignore_index=True)

    return tb_new, mask


def extract_tbs(tb):
    assert tb["hepatitis"].isna().all()
    assert tb["estimate"].isna().all()
    tb = tb.drop(columns=["hepatitis", "estimate"])

    # DIMENSIONS
    paths.log.info("GAM: processing dimensions")
    # 1/ Incorporate dimensional information from `dimension_0` into `group` and drop `dimension_0`
    tb = incorporate_dimension_0(tb)

    # 2/ Drop irrelevant columns
    tb = tb.drop(columns=["value_rounded", "data_denominator", "footnote"])

    # 3/ Ensure values in dimension `sex` (avoid None values)
    tb = handle_nulls_in_dimensions_gam(tb)

    # 4/ Get table with only `sex` and `group` dimensions.
    # This was not done in step "SEPARATE DATA", because we want to benefit from some of the processing done in this step. In particular, of the combination of `dimension_0` and `group`.

    # 4.3/ [sex, group]. Separate data with NO age
    tb_sex_group, mask_no_age = extract_sex_group_table_gam(tb)

    ## Correction
    # counts = tb.groupby(["sex", "age", "group", "indicator", "country", "year"]).size()
    masks = [
        {
            "mask": (
                (tb["country"] == "China")
                & (tb["year"] == 2021)
                & (tb["indicator"] == "syphilis_prevalence")
                & (tb["sex"] == "male")
                & (tb["age"] == "total")
                & (tb["group"] == "men who have sex with men")
            ),
            "mask_rm": (tb["dimension"] == "Total"),
        },
        {
            "mask": (
                (tb["country"] == "Democratic Republic of Congo")
                & (tb["year"] == 2022)
                & (tb["indicator"] == "experience_violence")
                & (tb["sex"] == "other")
                & (tb["age"] == "total")
                & (tb["group"] == "transgender")
            ),
            "mask_rm": (tb["dimension"] == "Females"),
        },
    ]
    for m in masks:
        assert len(tb[m["mask"]]) == 2
        assert tb.loc[m["mask"], "value"].nunique() == 1

        tb = tb.loc[~(m["mask"] & m["mask_rm"])]

    _ = tb.format(["country", "year", "indicator", "sex", "age", "group"])

    # Remove from main table!
    tb = tb.loc[~mask_no_age]

    return tb, tb_sex_group


def add_condoms_per_100k(tb_no_dim, tb_sex_group):
    tb_ = tb_sex_group.loc[
        (tb_sex_group["indicator"] == "condoms_distributed")
        & (tb_sex_group["sex"] == "total")
        & (tb_sex_group["group"] == "total")
    ]
    assert not tb_.empty, "Empty datafame!"
    tb_ = paths.regions.add_population(tb_)
    tb_["value"] = 100_000 * tb_["value"] / tb_["population"]
    tb_["indicator"] = "condoms_distributed_per_100k"
    tb_["sex"] = np.nan
    tb_["group"] = np.nan
    tb_ = tb_.drop(columns=["population"])

    tb_no_dim = pr.concat([tb_no_dim, tb_], ignore_index=True)
    return tb_no_dim


def pivot_and_format(tb, columns, short_name, regions_agg=True):
    # 1/ Save some fields (this might be useful later)
    tb_units = tb[["indicator", "unit"]].drop_duplicates()

    assert not tb_units[["indicator"]].duplicated().any(), "Multiple units for a single indicator!"

    # 1.5/ Ensure value column is numeric (not string)
    tb["value"] = pr.to_numeric(tb["value"], errors="coerce")

    # 2/ Pivot
    tb = tb.pivot(index=columns, columns="indicator", values="value").reset_index()
    tb = tb.underscore()

    # Convert StringDtype columns to object dtype to avoid issues with dropna
    for col in tb.columns:
        if hasattr(tb[col].dtype, "name") and "string" in str(tb[col].dtype):
            tb[col] = tb[col].astype(object)

    tb = tb.dropna(how="all")

    # 3/ Remove anomalies
    if short_name in ANOMALIES:
        paths.log.info(f"Removing anomalies from table: {short_name}")
        anomalies = ANOMALIES[short_name]
        tb = remove_anomalies(tb, anomalies)

    # Save unit info
    assert not tb_units["indicator"].duplicated().any(), "Multiple units or units for a single indicator!"

    # 4/ (OPTIONAL) Add regional aggregates
    # if columns_agg is not None:
    if regions_agg:
        columns_agg = tb_units.loc[tb_units["unit"] == "Number", "indicator"].tolist()
        # columns_agg = []
        if columns_agg != []:
            tb = add_regional_aggregates(
                tb,
                columns_agg=columns_agg,
            )

    # 5/ Format
    tb = tb.format(columns, short_name=short_name)

    return tb


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
        "avoidance_care",
        "condom_use",
        "experience_stigma",
        "experience_violence",
        "hiv_prevalence",
        "hiv_programmes_coverage",
        "hiv_status_awareness",
        "syphilis_prevalence",
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

    # TODO: age is NA?
    return tb


def handle_nulls_in_dimensions_gam(tb):
    """Make sure that there is no None/NA values in `sex`, `age`, `group` dimensions.

    Instead, we should use "total" when the value is not available.

    Detect dimensions with NA with:

    ```python
    indicators = sorted(tb.indicator.unique())
    dimension = "group"
    for indicator in indicators:
        x = _debug_highlight_none(tb, indicator, dimension)
        display(x)
    ```
    """
    # 1/ Fill `sex`
    # _debug_highlight_none(tb, "pwid_safety", "sex")
    ## keys: Indicators to set sex -> "total". values: dimensions expected to be None/NA
    set_sex_to_total = {
        "art_coverage": {
            "< 25",
            "25+",
        },
        "avoidance_care": {
            "< 25",
            "25+",
        },
        "condom_use": {
            "< 25",
            "25+",
        },
        "experience_stigma": {
            "< 25",
            "25+",
        },
        "experience_violence": {
            "< 25",
            "25+",
        },
        "hiv_prevalence": {
            "< 25",
            "25+",
        },
        "hiv_programmes_coverage": {
            "< 25",
            "25+",
        },
        "hiv_status_awareness": {
            "< 25",
            "25+",
        },
        "hiv_tests": {
            "Children (0-14)",
        },
        "pwid_safety": {
            "< 25",
            "25+",
        },
        "syphilis_prevalence": {
            "< 25",
            "25+",
        },
    }

    tb = safe_replace_NAs(tb, set_sex_to_total, "sex", "total")

    # 2/ Fill `age`
    # _debug_highlight_none(tb, "hiv_programmes_coverage", "age")
    set_age_to_total = {
        "art_coverage": {
            "Transgender",
            "Transman",
            "Transother",
            "Transwoman",
        },
        "avoidance_care": {
            "Transgender",
            "Transman",
            "Transother",
            "Transwoman",
        },
        "condom_use": {
            "Transgender",
            "Transman",
            "Transother",
            "Transwoman",
        },
        "experience_stigma": {
            "Transgender",
            "Transman",
            "Transother",
            "Transwoman",
        },
        "experience_violence": {
            "Transgender",
            "Transman",
            "Transother",
            "Transwoman",
        },
        "hiv_prevalence": {
            "Transgender",
            "Transman",
            "Transother",
            "Transwoman",
        },
        #
        "hiv_programmes_coverage": {
            "Transgender",
            "Transman",
            "Transother",
            "Transwoman",
        },
        "hiv_status_awareness": {
            "Transgender",
            "Transman",
            "Transother",
            "Transwoman",
        },
        "pwid_safety": {
            "Transgender",
        },
        "syphilis_prevalence": {
            "All sexes",
            "Transgender",
            "Transman",
            "Transother",
            "Transwoman",
        },
    }

    tb = safe_replace_NAs(tb, set_age_to_total, "age", "total")

    # 4/ Fill `group`
    # _debug_highlight_none(tb, "pwid_safety", "group")
    # sorted(tb.loc[tb.group.isna(), "indicator"].unique())
    set_group_to_total = {
        "hiv_tests": {
            "Children (0-14)",
            "Females Adults (15+)",
            "Males Adults (15+)",
        },
        "pwid_safety": {
            "< 25",
            "25+",
            "Females",
            "Males",
        },
    }

    tb = safe_replace_NAs(tb, set_group_to_total, "group", "total")

    #  5/ Check that there is no NA/None rows
    assert not tb.isna().any().any(), "Some NaNs were detected, but none were expected!"

    return tb


# COMMON
def clean_indicator_names(tb, short_name, drop: bool = True):
    column_indicator = "indicator"
    column_indicator_id = f"{column_indicator}_id"

    # Check indicators in table are known
    indicators_unknown = set(
        tb.loc[~tb[column_indicator_id].isin(INDICATOR_SHORT_NAME_MAPPING[short_name]), column_indicator_id].unique()
    )
    assert len(indicators_unknown) == 0, f"Unknown {len(indicators_unknown)} indicators found: {indicators_unknown}"

    # Keep relevant
    indicators_drop = [k for k, v in INDICATOR_SHORT_NAME_MAPPING[short_name].items() if v is None]
    paths.log.info(f"Dropping {len(indicators_drop)} indicators: %s", indicators_drop)
    tb = tb.loc[~tb[column_indicator_id].isin(indicators_drop)].reset_index(drop=True)

    # Rename indicators
    tb[column_indicator] = tb[column_indicator_id].map(INDICATOR_SHORT_NAME_MAPPING[short_name]).astype("string")
    assert tb[column_indicator].isna().sum() == 0, "NaNs found in indicator after renaming."

    # Drop unnecessary indicator ID column
    if drop:
        tb = tb.drop(columns=[column_indicator_id])

    return tb


def _check_dimensions(tb, dimensions, short_name):
    # Check dimensions
    ## Check 1
    dims = list(dimensions[short_name].keys())
    assert len(dims) == len(set(dims)), "Duplicated keys!"
    ## Check 2
    dims_expected = set(dimensions[short_name].keys())
    dims_found = set(tb["dimension_id"].unique())
    dims_unexpected = dims_found - dims_expected
    dims_not_found = dims_expected - dims_found
    assert len(dims_unexpected) == 0, f"Unexpected {len(dims_unexpected)} dimensions found: {dims_unexpected}"
    assert len(dims_not_found) == 0, f"Expected {len(dims_not_found)} dimensions not found: {dims_not_found}"


def get_all_countries():
    countries_all = []
    for region in REGIONS_TO_ADD:
        members = paths.regions.get_region(
            region,
            exclude_historical_countries=True,
        )["members"]
        countries_all.extend(members)
    countries_all = set(countries_all)
    return list(countries_all)


def expand_raw_dimension(tb, dimensions, dimension_names=None, drop=True, check_na: bool = False):
    """Add sex, age, estimate dimensions.

    sex: female, male, total
    age: age group range
    estimate: estimate, lower, upper
    """
    column_dimension = "dimension"
    column_dimension_id = f"{column_dimension}_id"

    if dimension_names is None:
        dimension_names = DIMENSION_COLUMNS
    # Add new dimension columns
    dim_values = {dim: {k: v.get(dim) for k, v in dimensions.items()} for dim in dimension_names}
    for dim in dimension_names:
        tb[dim] = tb[column_dimension_id].map(dim_values[dim]).astype("string")
        if check_na:
            assert tb[dim].isna().sum() == 0, f"NaNs found in dimension {dim} after expansion."

    # Drop old dimensions column
    if drop:
        tb = tb.drop(columns=[column_dimension])
        tb = tb.drop(columns=[column_dimension_id])

    return tb


def remove_anomalies(tb, anomalies):
    """Remove anomalies in the table."""
    for anomaly in anomalies:
        # Sanity checks
        assert "indicator" in anomaly
        assert "country" in anomaly
        assert anomaly["indicator"] in tb.columns, f"Indicator '{anomaly['indicator']}' not found in table!"
        # Create mask
        mask = tb["country"] == anomaly["country"]

        if "year" in anomaly:
            if isinstance(anomaly["year"], int):
                mask &= tb["year"] == anomaly["year"]
            elif isinstance(anomaly["year"], list):
                mask &= tb["year"].isin(anomaly["year"])
            else:
                raise TypeError("Unexpected type for 'year' in anomaly! Must be INT or LIST[INT].")

        if "dimensions" in anomaly:
            assert isinstance(
                anomaly["dimensions"], dict
            ), "Unexpected type for 'dimensions' in anomaly! Must be a DICT."
            for dim, value in anomaly["dimensions"].items():
                if isinstance(value, list):
                    mask &= tb[dim].isin(value)
                else:
                    mask &= tb[dim] == value

        # Remove anomalies
        tb.loc[mask, anomaly["indicator"]] = np.nan

    # Drop all NaN rows
    tb = tb.dropna(how="all")

    return tb


def add_regional_aggregates(tb, columns_agg):
    """
    Adding regional aggregates for all tuberculosis variables.
    """
    from etl.data_helpers import geo

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

    # Create a table for each disaggregation value
    # Add region aggregates.
    # print(index_columns)
    # print([col for col in tb_agg.columns if col not in index_columns])
    # print(tb_agg.dtypes)
    tb_agg = geo.add_regions_to_table(
        tb=tb_agg,
        ds_regions=paths.regions.ds_regions,
        ds_income_groups=paths.regions.ds_income_groups,
        regions=REGIONS_TO_ADD,
        index_columns=index_columns,
        min_num_values_per_year=1,
        frac_allowed_nans_per_year=0.3,
        countries_that_must_have_data={
            "Asia": ["China", "India"],
            "North America": ["United States", "Canada", "Mexico"],
            "Europe": ["Germany", "France", "United Kingdom", "Italy", "Spain"],
            "Africa": ["Nigeria", "Ethiopia", "Kenya"],
            "South America": ["Brazil", "Argentina", "Colombia"],
            "Oceania": ["Australia", "New Zealand"],
            "High-income countries": ["United States", "Germany", "Japan", "United Kingdom", "France"],
            "Upper-middle-income countries": ["China", "Brazil", "Mexico", "South Africa", "Indonesia"],
            "Lower-middle-income countries": ["India", "Pakistan", "Bangladesh", "Philippines", "Nigeria", "Kenya"],
            "Low-income countries": ["Ethiopia", "Uganda"],
        },
    )

    # Drop unnecessary rows
    tb_agg = tb_agg.dropna(subset=columns_agg, how="all")

    # Merge with table without aggregates
    tb = pr.merge(tb_no_agg, tb_agg, on=index_columns, how="outer").reset_index(drop=True)

    return tb


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
    cols_show = cols + ["dimension", "unit"] + list(tb.columns.intersection(["dimension_0"]))
    x = tb.loc[tb["indicator"] == indicator_name, ["indicator"] + cols_show].drop_duplicates().sort_values(cols)
    condition = tb[column].isna()
    x = x.style.apply(highlight_row, condition=condition, axis=1)
    # display(x)
    return x
