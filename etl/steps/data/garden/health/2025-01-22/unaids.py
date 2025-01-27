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
    "q_a_120",
    "q_a_121",
    "q_a_123",
    "q_a_125",
    "q_a_65",
    "q_a_67",
    "q_a_68",
    "q_a_6b",
    "q_a_70",
]
INDICATORS_GAM_DROP = [
    "income_status",
    "unaids_rsts",
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

    # Drop special indicators
    INDICATORS_GAM_REMOVE = ["GEOGRAPHICAL_REGIONS", "COUNTRY_OFFICES"]
    tb = tb.loc[~tb["indicator"].isin(INDICATORS_GAM_REMOVE)]

    # TODO: review dimensions per indicator
    # Harmonize
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
        excluded_countries_file=paths.excluded_countries_path,
        make_missing_countries_nan=True,
    )

    # Collapse original indicators into indicator + dimension
    dix = {}
    for dim in dimensions_collapse["gam"]:
        _dix = {k: {**v, "name": dim["name"]} for k, v in dim["indicators_origin"].items()}
        dix |= _dix
    tb["dimension_0"] = tb["indicator"].map(lambda x: dix[x]["dimension"] if x in dix else None)
    tb["indicator"] = tb["indicator"].map(lambda x: dix[x]["name"] if x in dix else x)
    ## Lower case indicator names
    tb["indicator"] = tb["indicator"].str.lower()

    # Drop non-relevant (or non-supported) indicators
    tb = tb.loc[~tb["indicator"].isin(INDICATORS_GAM_DROP + INDICATORS_GAM_CATEGORICAL)]
    tb = tb.loc[~((tb["indicator"] == "population") & (tb["dimension"] == "TOTAL"))]

    # Work In Progress
    tb = extract_and_add_dimensions(
        tb, dimensions["gam"], dimension_names=["sex", "age", "group", "hepatitis", "estimate"], drop=False
    )

    #######
    ####### SEPARATE DATA HEPATITIS / ESTIMATES
    #######
    # Separate hepatitis data
    indicators_hepatitis = set(tb.dropna(subset="hepatitis")["indicator"].unique())
    assert indicators_hepatitis == {"viral_hepatitis"}, "Unexpected extra indicators!"
    mask_hepatitis = tb["indicator"].isin(indicators_hepatitis)
    tb_hepatitis = tb.loc[mask_hepatitis]
    # Checks
    assert set(tb_hepatitis["sex"].unique()) == {"female", "male", "total"}
    assert set(tb_hepatitis["age"].unique()) == {"0-25", "25+", "total"}
    assert set(tb_hepatitis["hepatitis"].unique()) == {"B", "C"}
    assert tb_hepatitis["group"].notna().sum() == 4, "Unexpected not-NAs"
    # Drop columns
    tb_hepatitis = tb_hepatitis.drop(columns=["estimate"])
    # Add group
    tb_hepatitis = tb_hepatitis.loc[tb_hepatitis["group"].isna()]
    tb_hepatitis["group"] = tb_hepatitis["dimension_0"].fillna("total")

    # Separate estimate data
    indicators_estimate = set(tb.dropna(subset="estimate")["indicator"].unique())
    assert indicators_estimate == {
        "population",
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
    # Drop columns
    tb_estimate = tb_estimate.drop(columns=["sex", "age", "hepatitis"])
    # Add group
    tb_estimate["group"] = tb_estimate["dimension_0"].fillna("total")

    ##############

    # Back to main table
    tb = tb.loc[~(mask_hepatitis | mask_estimate)]
    assert tb["hepatitis"].isna().all()
    assert tb["estimate"].isna().all()
    tb = tb.drop(columns=["hepatitis", "estimate"])

    # FIX dimensions
    ## sex
    # tb.sex.value_counts(dropna=False)
    ## age
    # tb.age.value_counts(dropna=False)

    ## group
    tb["group"] = tb["group"].fillna(tb["dimension_0"])
    mask = (tb["group"].notna()) & (tb["group"] == "total") & (tb["dimension_0"].notna())
    tb.loc[mask, "group"] = tb.loc[mask, "dimension_0"]
    ## TODO: Find solution for (tb["group"].notna()) & (tb["group"] == "total") & (tb["dimension_0"].notna())
    # tb[(tb.group.notna()) & (tb.group != "total") & (tb.dimension_0.notna())].indicator.unique()
    # array(['hiv_tests', 'hiv_prevalence', 'condom_use',
    #    'hiv_status_awareness', 'hiv_programmes_coverage',
    #    'avoidance_care', 'art_coverage', 'syphilis_prevalence'],
    #   dtype=object)
    # TODO:
    # 1) hiv_tests -> new table
    # 2) The rest, keep and join as "group, dimension_0". Look at the data for transgender/transwoman/transman/transother

    # Explore
    # 1) Nothing in group, nothing in dimension_0 => NO ACTION
    tb_1 = tb[(tb.group.isna()) & (tb.dimension_0.isna())]
    print(len(tb_1))

    # 2) Something in group, nothing in dimension_0 => NO ACTION
    tb_2 = tb[(tb.group.notna()) & (tb.dimension_0.isna())]
    print(len(tb_2))

    # 2) Nothing in group, something in dimension_0 => EASY REPLACE
    tb_3 = tb[(tb.group.isna()) & (tb.dimension_0.notna())]
    print(len(tb_3))

    # 4) Something in group, something in dimension_0 => HARD REPLACE
    tb_4 = tb[(tb.group.notna()) & (tb.dimension_0.notna())]
    print(len(tb_4))

    # 'unaids_rsts']
    # tb_gam = make_table_gam(tb_gam)
    # tb_gam = tb_gam.format(["country", "year", "age", "sex", "estimate"], short_name="epi")

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
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb],
        default_metadata=ds_meadow.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def make_table_epi(tb, dimensions):
    # Add dimensions

    tb = extract_and_add_dimensions(tb, dimensions)

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
    tb_meta["indicator"] = tb_meta["indicator"]
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


def extract_and_add_dimensions(tb, dimensions, dimension_names=None, drop=True):
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
