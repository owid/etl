"""V-Dem Democracy Dataset Aggregation Pipeline.

This module processes V-Dem democracy indicators and creates multiple aggregated datasets:
1. Country-based aggregates (counts and averages by region)
2. Population-weighted aggregates (counts and averages by region)
3. Unified tables with and without regional data
4. Tables split by indicator dimensionality (uni vs multi-dimensional)

The main workflow:
- Takes raw V-Dem data with democracy indicators by country-year
- Creates dummy variables for categorical indicators
- Aggregates to regional and global levels using two methods:
  * Simple country averages (each country weighted equally)
  * Population-weighted averages (larger countries weighted more)
- Produces final tables suitable for charting and analysis
"""

from itertools import chain
from typing import cast

import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.tables import concat
from shared import add_population_in_dummies, expand_observations, from_wide_to_long

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# REGION DEFINITIONS FOR AGGREGATION
# Defines which countries belong to each region, including historical entities
# that may not be in standard regional classifications
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
            "Baden",
            "Bavaria",
            "Brunswick",
            "Duchy of Nassau",
            "Hamburg",
            "Hanover",
            "Hesse Electoral",
            "Hesse Grand Ducal",
            "Mecklenburg Schwerin",
            "Modena",
            "Oldenburg",
            "Parma",
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

# THRESHOLDS to consider a region as having enough data for aggregation.
## Share of countries in region required to estimate regional averages
THRESHOLD_SHARE_COUNTRIES = 2 / 3
## Share of people living regions required to estimate regional averages
THRESHOLD_SHARE_POPULATION = 2 / 3

# Reference year for coverage of countries
REFERENCE_YEAR = 1900

# INDICATORS FOR REGIONAL AVERAGING
# These V-Dem indicators will have regional averages calculated using both
# simple country averages and population-weighted averages
_indicators_avg = [
    "civ_libs_vdem",
    "civ_soc_str_vdem",
    "civsoc_particip_vdem",
    "corr_exec_vdem",
    "corr_jud_vdem",
    "corr_leg_vdem",
    "corr_publsec_vdem",
    "corruption_vdem",
    "counterarg_polch_vdem",
    "civ_libs_vdem",
    "civ_soc_str_vdem",
    "delib_vdem",
    "delibdem_vdem",
    "dom_auton_vdem",
    "egal_vdem",
    "egaldem_vdem",
    "electdem_vdem",
    "electfreefair_vdem",
    "electoff_vdem",
    "elitecons_polch_vdem",
    "equal_access_vdem",
    "equal_res_vdem",
    "equal_rights_vdem",
    "freeassoc_vdem",
    "freeexpr_vdem",
    "indiv_libs_vdem",
    "int_auton_vdem",
    "judicial_constr_vdem",
    "justcomgd_polch_vdem",
    "justified_polch_vdem",
    "legis_constr_vdem",
    "lib_vdem",
    "libdem_vdem",
    "locelect_vdem",
    "particip_vdem",
    "participdem_vdem",
    "personalism_vdem",
    "phys_integr_libs_vdem",
    "pol_libs_vdem",
    "priv_libs_vdem",
    "public_admin_vdem",
    "regelect_vdem",
    "rule_of_law_vdem",
    "soccons_polch_vdem",
    "socgr_civ_libs_vdem",
    "socgr_pow_vdem",
    "suffr_vdem",
    "terr_contr_vdem",
    "turnout_total_vdem",
    "turnout_vdem",
    "wom_emp_vdem",
    "wom_civ_libs_vdem",
    "wom_civ_soc_vdem",
    "wom_emp_vdem",
    "wom_parl_vdem",
    "wom_pol_par_vdem",
    # New
    "v2xca_academ",
    "v2mecenefm",
    "v2meharjrn",
    "v2meslfcen",
    "v2cademmob",
    "v2caautmob",
    "v2cacamps",
    "v2caviol",
    # New 2025-05-26
    "v2mebias",
    "v2smgovdom",
    "v2cagenmob",
    "v2xcl_prpty",
    "v2mecorrpt",
    "v2xnp_client",
    "v2elvotbuy",
]
INDICATORS_REGION_AVERAGES = [[f"{ind_name}{dim}" for dim in ["", "_low", "_high"]] for ind_name in _indicators_avg]
INDICATORS_REGION_AVERAGES = list(chain.from_iterable(INDICATORS_REGION_AVERAGES)) + ["wom_parl_vdem"]

# For a sanity check on table shape
N_EXPECTED = 196

# Indicators that should not have regional averages pre-1900 [ref: https://github.com/owid/owid-issues/issues/1963#issuecomment-3139107273]
INDICATORS_NO_AGG_PRE_1900 = [
    "corruption_vdem",
    "corr_exec_vdem",
    "corr_publsec_vdem",
    "corr_leg_vdem",
    "corr_jud_vdem",
    "v2mecorrpt",
    "v2xnp_client",
]


def run(tb: Table, ds_regions: Dataset, ds_population: Dataset) -> tuple[Table, Table, Table, Table, Table, Table]:
    """Main aggregation pipeline for V-Dem democracy data.

    Processes raw V-Dem democracy indicators and creates multiple aggregated datasets
    with regional and global aggregates using two different weighting methods.

    Args:
        tb: Quasi-raw V-Dem data table with democracy indicators by country-year
        ds_regions: Dataset containing regional classifications
        ds_population: Dataset containing population data for weighting

    Returns:
        Tuple of 6 tables:
        - tb_uni_without_regions: Uni-dimensional indicators, countries only
        - tb_uni_with_regions: Uni-dimensional indicators with regional aggregates
        - tb_multi_without_regions: Multidimensional indicators, countries only
        - tb_multi_with_regions: Multidimensional indicators with regional aggregates
        - tb_countries_counts: Country counts by regime type and region
        - tb_population_counts: Population counts by regime type and region

    Example:
        Input: Raw V-Dem data with indicators like electoff_vdem, civ_libs_vdem
        Output: 6 tables ready for charting with country and regional data
    """
    tb_ = tb.copy()

    # Create country-based aggregates (each country weighted equally)
    tb_countries_counts, tb_countries_avg = make_table_countries(tb_, ds_regions)

    # Create population-weighted aggregates (larger countries have more weight)
    tb_population_counts, tb_population_avg = make_table_population(
        tb_,
        ds_regions,
        ds_population=ds_population,
    )

    # Remove some regional aggregates [ref: https://github.com/owid/owid-issues/issues/1963#issuecomment-3139107273]

    # World: no global data needed at all.
    tb_countries_avg.loc[tb_countries_avg["country"] == "World", INDICATORS_NO_AGG_PRE_1900] = pd.NA
    tb_population_avg.loc[tb_population_avg["country"] == "World", INDICATORS_NO_AGG_PRE_1900] = pd.NA
    # Regional averages before 1900
    tb_countries_avg.loc[tb_countries_avg["year"] < 1900, INDICATORS_NO_AGG_PRE_1900] = pd.NA
    tb_population_avg.loc[tb_population_avg["year"] < 1900, INDICATORS_NO_AGG_PRE_1900] = pd.NA

    # Prepare main data and split into output tables by dimensionality
    tb_ = tb_.drop(columns=["regime_imputed_country", "regime_imputed", "histname"])
    tb_uni_without_regions, tb_uni_with_regions, tb_multi_without_regions, tb_multi_with_regions = make_main_tables(
        tb_, tb_countries_avg, tb_population_avg
    )

    # Ensure consistent metadata origins across all columns (V-Dem as data source)
    # For multidimensional indicators table
    origin = tb_multi_with_regions["civ_libs_vdem"].m.origins[0]
    assert origin.producer == "V-Dem", "Assigned origin should be V-Dem!"
    for col in tb_multi_with_regions.columns:
        tb_multi_with_regions[col].metadata.origins = [origin]
    # For unidimensional indicators table
    origin = tb_uni_with_regions["electoff_vdem"].m.origins[0]
    assert origin.producer == "V-Dem", "Assigned origin should be V-Dem!"
    for col in tb_uni_with_regions.columns:
        tb_uni_with_regions[col].metadata.origins = [origin]

    return (
        tb_uni_without_regions,
        tb_uni_with_regions,
        tb_multi_without_regions,
        tb_multi_with_regions,
        tb_countries_counts,
        tb_population_counts,
    )


# %% NUM_COUNTRIES TABLES
def make_table_countries(tb: Table, ds_regions: Dataset) -> tuple[Table, Table]:
    """Create country-based regional aggregates using simple averages.

    Generates two types of regional aggregates:
    1. Country counts by regime type (e.g., number of democracies in Europe)
    2. Simple averages of democracy indicators (each country weighted equally)

    Args:
        tb: V-Dem data table with country-year observations
        ds_regions: Dataset containing regional classifications

    Returns:
        Tuple of (counts_table, averages_table):
        - counts_table: Number of countries in each category by region-year
        - averages_table: Simple averages of democracy indicators by region-year

    Example:
        Input: V-Dem data for USA, Canada, Mexico with democracy scores
        Output: North America average democracy score (USA + Canada + Mexico) / 3
    """
    # Remove imputed countries (they did not exist, so should not count them!)
    tb_ = tb.loc[~tb["regime_imputed"]].copy()

    # Convert country to string for consistency
    tb_["country"] = tb_["country"].astype("string")

    # Validate that all countries in data are assigned to regions
    # This prevents missing countries from being ignored in regional aggregates
    members_tracked = set()
    for region, region_props in REGIONS.items():
        members_tracked |= set(
            geo.list_members_of_region(region, ds_regions, additional_members=region_props.get("additional_members"))
        )
    countries_found = set(tb_["country"])
    countries_nottracked = countries_found - members_tracked
    assert not countries_nottracked, f"Some countries are not in the regions: {countries_nottracked}!"

    # Generate counts of countries in X category
    tb_sum = make_table_countries_counts(tb_, ds_regions)
    # Generate averages in regions (over countries) in X category
    tb_avg = make_table_countries_avg(tb_, ds_regions)

    # Merge tb_sum and tb_avg. Sanity-check that there is no overlap in columns (except for index)
    assert set(tb_sum.columns).intersection(set(tb_avg.columns)) == {"year", "country"}, "Unexpected column overlap!"
    # tb_ = tb_sum.merge(tb_avg, on=["country", "year"], how="outer")

    return tb_sum, tb_avg


def make_table_countries_counts(tb: Table, ds_regions: Dataset) -> Table:
    """Calculate number of countries in each regime category by region.

    Creates dummy variables for regime types and aggregates by region to count
    how many countries fall into each category (e.g., liberal democracy, autocracy).

    Args:
        tb: V-Dem data with regime classifications
        ds_regions: Dataset for regional aggregation

    Returns:
        Table with country counts by regime type and region-year

    Example:
        Output: "5 countries in Europe are liberal democracies in 2020"
    """
    tb_ = tb.copy()
    # Generate dummy indicators
    tb_ = make_table_with_dummies(tb_)

    # Estimate region aggregates
    tb_ = add_regions_and_global_aggregates(tb_, ds_regions)

    # Sanity check on output shape
    assert tb_.shape[1] == 60, f"Unexpected number of columns {tb_.shape[1]}."

    # Wide to long format
    tb_ = from_wide_to_long(tb_)

    # Remove some dimensions
    tb_.loc[
        tb_["category"].isin(["closed autocracy", "electoral autocracy", "electoral democracy"]),
        ["num_countries_years_in_electdem", "num_countries_years_in_libdem"],
    ] = float("nan")

    # Remove data pre-1900 for num_countries_wom_parl
    tb_.loc[tb_["year"] < REFERENCE_YEAR, "num_countries_wom_parl"] = float("nan")

    return tb_


def make_table_countries_avg(tb: Table, ds_regions: Dataset) -> Table:
    """Calculate simple regional averages of democracy indicators.

    Computes unweighted averages where each country contributes equally,
    regardless of population size (e.g., Luxembourg = Germany = 1 vote).

    Args:
        tb: V-Dem data with democracy indicators
        ds_regions: Dataset for regional aggregation

    Returns:
        Table with regional averages of democracy indicators

    Example:
        Europe average civil liberties = (Germany_score + France_score + ...) / num_countries
    """
    tb_ = tb.copy()

    # Keep only relevant columns
    cols_indicators = [col for col in tb_.columns if col in INDICATORS_REGION_AVERAGES]
    tb_ = tb_.loc[:, ["year", "country"] + cols_indicators]

    # TODO: aggregations encodes the logic of: "estimate mean if >70% of 1900 countries are present in the region"
    # Get list of countries in regions in 1900
    tb_1900 = tb_.loc[tb_["year"] == REFERENCE_YEAR, ["country"]]
    countries_to_continent = geo.countries_to_continent_mapping(
        ds_regions=ds_regions,
        regions=REGIONS,
        exclude_historical_countries=False,
        include_historical_regions_in_income_groups=True,
    )
    tb_1900["continent"] = tb_1900["country"].map(countries_to_continent)
    countries_must_have_data = tb_1900.groupby("continent")["country"].agg(list).to_dict()
    frac_must_have_data = {region: THRESHOLD_SHARE_COUNTRIES for region in countries_must_have_data.keys()} | {
        "Europe": 0.1
    }

    # Estimate region aggregates
    tb_ = add_regions_and_global_aggregates(
        tb=tb_,
        ds_regions=ds_regions,
        aggregations={k: "mean" for k in cols_indicators},  # type: ignore
        aggregations_world={k: "mean" for k in cols_indicators},  # type: ignore
        countries_must_have_data=countries_must_have_data,
        frac_must_have_data=frac_must_have_data,
    )

    # Sanity check on output shape
    assert tb_.shape[1] == N_EXPECTED, f"Unexpected number of columns. Expected {N_EXPECTED} but found {tb_.shape[1]}"

    return tb_


# %% POPULATION TABLES
def make_table_population(tb: Table, ds_regions: Dataset, ds_population: Dataset) -> tuple[Table, Table]:
    """Create population-weighted regional aggregates.

    Generates two types of population-based aggregates:
    1. Total population living under each regime type by region
    2. Population-weighted averages of democracy indicators

    Args:
        tb: V-Dem data table
        ds_regions: Dataset for regional aggregation
        ds_population: Population data for weighting

    Returns:
        Tuple of (population_counts, population_weighted_averages)

    Example:
        Input: Germany (80M people, score=0.8), Luxembourg (0.6M people, score=0.9)
        Output: Europe weighted average = (80M*0.8 + 0.6M*0.9) / 80.6M = 0.801
    """
    tb_ = tb.copy()

    # Drop historical countries (don't want to double-count population)
    tb_ = expand_observations_without_leading_to_duplicates(tb_)

    # Generate counts of people in X category
    tb_sum = make_table_population_counts(tb_, ds_regions, ds_population)
    # Generate averages of countries in X category
    tb_avg = make_table_population_avg(tb_, ds_regions, ds_population)

    # Merge tb_sum and tb_avg. Sanity-check that there is no overlap in columns (except for index)
    assert set(tb_sum.columns).intersection(set(tb_avg.columns)) == {"year", "country"}, "Unexpected column overlap!"

    return tb_sum, tb_avg


def make_table_population_counts(tb: Table, ds_regions: Dataset, ds_population: Dataset) -> Table:
    """Calculate total population living under each regime type by region.

    Multiplies dummy variables by population data to count people instead of countries.
    Provides population-based perspective on democracy prevalence.

    Args:
        tb: V-Dem data with regime classifications
        ds_regions: Regional classification dataset
        ds_population: Population data for weighting

    Returns:
        Table with population counts by regime type and region

    Example:
        Output: "500 million people live in liberal democracies in Europe"
    """
    tb_ = tb.copy()

    # Get dummy indicators
    tb_ = make_table_with_dummies(tb_, people_living_in=True)

    # Add population in dummies (population value replaces 1, 0 otherwise)
    tb_ = add_population_in_dummies(
        tb_,
        ds_population,
        expected_countries_without_population=[
            # Germany
            "Baden",
            "Bavaria",
            "Brunswick",
            "Duchy of Nassau",
            "Hamburg",
            "Hanover",
            "Hesse Electoral",
            "Hesse Grand Ducal",
            "Mecklenburg Schwerin",
            "Oldenburg",
            "Saxony",
            "Saxe-Weimar-Eisenach",
            "Wurttemberg",
            "Zanzibar",
            # Italy
            "Tuscany",
            "Modena",
            "Two Sicilies",
            "Parma",
            "Piedmont-Sardinia",
            # Others
            "Somaliland",
            "Palestine/Gaza",
            "Palestine/West Bank",
            "Democratic Republic of Vietnam",
            "Republic of Vietnam",
        ],
    )

    # Get region aggregates
    tb_ = add_regions_and_global_aggregates(
        tb=tb_,
        ds_regions=ds_regions,
    )

    # Sanity check on output shape
    assert tb_.shape[1] == 61, f"Unexpected number of columns {tb_.shape[1]}."

    # Long format
    tb_ = from_wide_to_long(tb_)

    # Rename columns
    tb_ = tb_.rename(
        columns={
            "num_countries_hoe": "population_hoe",
            "num_countries_hog": "population_hog",
            "num_countries_hos": "population_hos",
            "num_countries_regime": "population_regime",
            "num_countries_regime_amb": "population_regime_amb",
            "num_countries_wom_parl": "population_wom_parl",
            "num_countries_years_in_electdem": "population_years_in_electdem",
            "num_countries_years_in_libdem": "population_years_in_libdem",
            "num_countries_natelect": "population_natelect",
            "num_countries_wom_hoe_ever": "population_wom_hoe_ever",
            "num_countries_wom_hoe_ever_demelect": "population_wom_hoe_ever_demelect",
        }
    )

    # Remove some dimensions
    tb_.loc[
        tb_["category"].isin(["closed autocracy", "electoral autocracy", "electoral democracy"]),
        ["population_years_in_electdem", "population_years_in_libdem"],
    ] = float("nan")
    return tb_


def make_table_population_avg(tb: Table, ds_regions: Dataset, ds_population: Dataset) -> Table:
    """Calculate population-weighted regional averages of democracy indicators.

    Weights each country by its population when calculating regional averages,
    giving larger countries more influence in the regional score.

    Args:
        tb: V-Dem data with democracy indicators
        ds_regions: Regional classification dataset
        ds_population: Population data for weighting

    Returns:
        Table with population-weighted regional averages

    Example:
        China (1.4B people) has more weight than Singapore (6M people) in Asia average
    """
    tb_ = tb.copy()

    # Keep only relevant columns
    cols_indicators = [col for col in tb_.columns if col in INDICATORS_REGION_AVERAGES]
    tb_ = tb_.loc[:, ["year", "country"] + cols_indicators]

    # Initialize table to estimate (%) of population covered
    tb_pop = tb_.copy()
    cols_to_transform = [col for col in tb_.columns if col not in ["year", "country"]]
    tb_pop[cols_to_transform] = tb_pop[cols_to_transform].notna().astype(int)

    # Add population in dummies (population value replaces 1, 0 otherwise)
    kwargs_dummies = {
        "ds_population": ds_population,
        "expected_countries_without_population": [
            # Germany
            "Baden",
            "Bavaria",
            "Brunswick",
            "Duchy of Nassau",
            "Hamburg",
            "Hanover",
            "Hesse Electoral",
            "Hesse Grand Ducal",
            "Mecklenburg Schwerin",
            "Oldenburg",
            "Saxony",
            "Saxe-Weimar-Eisenach",
            "Wurttemberg",
            "Zanzibar",
            # Italy
            "Tuscany",
            "Modena",
            "Two Sicilies",
            "Parma",
            "Piedmont-Sardinia",
            # Others
            "Somaliland",
            "Palestine/Gaza",
            "Palestine/West Bank",
            "Democratic Republic of Vietnam",
            "Republic of Vietnam",
        ],
        "drop_population": False,
    }
    tb_ = add_population_in_dummies(tb_, **kwargs_dummies)

    # Get region aggregates
    kwargs_agg = {
        "ds_regions": ds_regions,
        "aggregations": {k: "sum" for k in cols_indicators} | {"population": "sum"},
        "min_num_values_per_year": 1,  # Ensure at least one country contributes to the average
    }
    tb_ = add_regions_and_global_aggregates(
        tb=tb_,
        **kwargs_agg,
    )

    # Normalize by region's population
    columns_index = ["year", "country"]
    columns_indicators = [col for col in tb_.columns if col not in columns_index + ["population"]]
    tb_[columns_indicators] = tb_[columns_indicators].div(tb_["population"], axis=0)
    tb_ = tb_.drop(columns="population")

    # Rename columns
    # tb_ = tb_.rename(columns={col: f"popw_{col}" for col in INDICATORS_REGION_AVERAGES})
    # Sanity check on output shape
    assert tb_.shape[1] == N_EXPECTED, f"Unexpected number of columns. Expected {N_EXPECTED} but found {tb_.shape[1]}"

    # Filter by coverage of people
    ## Get dummy table
    tb_pop = add_population_in_dummies(tb_pop, **kwargs_dummies)
    # tb_pop = tb_pop.drop(columns=["population"])
    ## Get population covered (absolute)
    tb_pop = add_regions_and_global_aggregates(
        tb=tb_pop,
        **kwargs_agg,
    )
    ## Get population covered (%)
    tb_pop.loc[:, columns_indicators] = tb_pop.loc[:, columns_indicators].div(tb_pop["population"], axis=0)
    tb_pop = tb_pop.drop(columns="population")
    ## Get flag if population coverage is above threshold
    tb_pop[columns_indicators] = tb_pop[columns_indicators] >= THRESHOLD_SHARE_POPULATION

    # Filter tb_ by population coverage
    tb_[columns_indicators] = tb_[columns_indicators].where(tb_pop[columns_indicators])

    return tb_


def expand_observations_without_leading_to_duplicates(tb: Table) -> Table:
    """Handle country transitions to avoid double-counting population.

    Manages historical country transitions (e.g., USSR→Russia, West/East Germany→Germany)
    to ensure population isn't double-counted during transition periods.

    Args:
        tb: V-Dem data with potential country overlaps

    Returns:
        Table with resolved country transitions and no duplicate population

    Example:
        Input: Both "USSR" and "Russia" data for 1991
        Output: Only "Russia" data for 1991 (USSR dropped to avoid double-counting)
    """
    # Extend observations to have all country-years
    tb = expand_observations(tb)

    # Drop former and current countries for some periods of years
    ## We've kept countries that were two sides of a current country (need to keep them since each side could have different regimes)
    ## West and East Germany, North and South Yemen, North and South Vietnam
    YEARS_YEMEN = (1918, 1990)
    YEARS_GERMANY = (1949, 1990)
    YEARS_VIETNAM = (1945, 1975)
    tb = tb.loc[
        ~(
            # Yemen
            ((tb["country"] == "Yemen Arab Republic") & ((tb["year"] > YEARS_YEMEN[1]) | (tb["year"] < YEARS_YEMEN[0])))
            | (
                (tb["country"] == "Yemen People's Republic")
                & ((tb["year"] > YEARS_YEMEN[1]) | (tb["year"] < YEARS_YEMEN[0]))
            )
            | ((tb["country"] == "Yemen") & (tb["year"] >= YEARS_YEMEN[0]) & (tb["year"] <= YEARS_YEMEN[1]))
            # Germany
            | ((tb["country"] == "West Germany") & ((tb["year"] > YEARS_GERMANY[1]) | (tb["year"] < YEARS_GERMANY[0])))
            | ((tb["country"] == "East Germany") & ((tb["year"] > YEARS_GERMANY[1]) | (tb["year"] < YEARS_GERMANY[0])))
            | ((tb["country"] == "Germany") & (tb["year"] >= YEARS_GERMANY[0]) & (tb["year"] <= YEARS_GERMANY[1]))
            # Vietnam
            | ((tb["country"] == "Republic of Vietnam") & (tb["year"] > YEARS_VIETNAM[1]))
            | (
                (tb["country"] == "Democratic Republic of Vietnam")
                & ((tb["year"] > YEARS_VIETNAM[1]) | (tb["year"] < YEARS_VIETNAM[0]))
            )
            | ((tb["country"] == "Vietnam") & (tb["year"] <= YEARS_VIETNAM[1]))
        )
    ]

    # Replace "Republic of Vietnam" -> "Vietnam" for years before 1945
    tb.loc[(tb["country"] == "Republic of Vietnam") & (tb["year"] < YEARS_VIETNAM[0]), "country"] = "Vietnam"

    # Sanity checks
    country_overlaps = [
        ("Republic of Vietnam", "Vietnam"),
        ("Democratic Republic of Vietnam", "Vietnam"),
        ("Republic of Vietnam", "Vietnam"),
        ("Yemen Arab Republic", "Yemen"),
        ("Yemen People's Republic", "Yemen"),
        ("West Germany", "Germany"),
        ("East Germany", "Germany"),
    ]
    for countries in country_overlaps:
        assert tb.loc[tb.country.isin(countries)].groupby("year").size().max() == 1, f"Overlap found for {countries}!"

    return tb


# %% MAIN TABLES
def make_main_tables(tb: Table, tb_countries_avg: Table, tb_population_avg: Table) -> tuple[Table, Table, Table, Table]:
    """Create final tables combining country data with regional aggregates.

    Splits indicators into uni-dimensional vs multidimensional, then combines
    country-level data with both simple and population-weighted regional averages.

    Args:
        tb: Base country-level V-Dem data
        tb_countries_avg: Simple regional averages
        tb_population_avg: Population-weighted regional averages

    Returns:
        Tuple of 4 tables:
        - Uni-dimensional indicators without regions (country data only)
        - Uni-dimensional indicators with regions (includes regional aggregates)
        - Multidimensional indicators without regions
        - Multidimensional indicators with regions

    Example:
        Unidimensional: Single democracy score per country-year
        Multidimensional: Democracy score with confidence intervals (low/best/high)
    """
    # 0/ Sanity check on regions
    assert set(tb_countries_avg["country"].unique()) == REGIONS.keys() | {
        "World"
    }, "Countries in tb_countries_avg do not match defined regions!"
    assert set(tb_population_avg["country"].unique()) == REGIONS.keys() | {
        "World"
    }, "Countries in tb_countries_avg do not match defined regions!"

    # 1/ Get uni- and multi-dimensional indicator tables
    ## It puts indicators that have '_low' in the name in the multi-dimensional table. It also formats it into "long format", so to have a new column `estimate`.
    tb_uni, tb_multi = _split_into_uni_and_multi(tb)

    # 2/ Re-shape tables with region averages (WIDE -> LONG)
    tb_countries_avg = from_wide_to_long(
        tb_countries_avg,
        indicator_name_callback=lambda x: x.replace("_low", "").replace("_high", ""),
        indicator_category_callback=lambda x: "low" if "_low" in x else "high" if "_high" in x else "best",
        column_dimension_name="estimate",
    )
    tb_population_avg = from_wide_to_long(
        tb_population_avg,
        indicator_name_callback=lambda x: x.replace("_low", "").replace("_high", ""),
        indicator_category_callback=lambda x: "low" if "_low" in x else "high" if "_high" in x else "best",
        column_dimension_name="estimate",
    )
    assert set(tb_population_avg) == set(
        tb_countries_avg
    ), "Columns in tb_population_avg and tb_countries_avg do not match!"

    # 3/ Get columns of indicators with region data, based on whether they have estimates (dimension estimate) or not
    columns_uni = [
        col
        for col in tb_uni.columns
        if (col in tb_countries_avg.columns and col not in {"country", "year", "estimate"})
    ]
    columns_multi = [
        col
        for col in tb_multi.columns
        if (col in tb_countries_avg.columns and col not in {"country", "year", "estimate"})
    ]

    # 4/ Bake tb_uni_*
    ## Initiate two tables with and without regions
    cols_index = ["country", "year"]
    tb_uni_with_regions = tb_uni.loc[:, cols_index + columns_uni].copy()
    tb_uni_without_regions = tb_uni.drop(columns=columns_uni).copy()

    ## Get columns from tb_*_avg tables relevant
    tb_countries_avg_uni = tb_countries_avg.loc[:, cols_index + columns_uni].dropna(subset=columns_uni, how="all")
    tb_population_avg_uni = tb_population_avg.loc[:, cols_index + columns_uni].dropna(subset=columns_uni, how="all")

    ## Add suffix (population-weighted) where applicable
    tb_population_avg_uni["country"] = tb_population_avg_uni["country"] + " (population-weighted)"

    ## Concatenate and create tb_uni_with_regions
    tb_uni_with_regions = concat(
        [
            tb_uni_with_regions,
            tb_countries_avg_uni,
            tb_population_avg_uni,
        ],
        ignore_index=True,
    )

    # 5/ Bake tb_multi_*
    ## Initiate two tables with and without regions
    cols_index = ["country", "year", "estimate"]
    tb_multi_with_regions = tb_multi.loc[:, cols_index + columns_multi].copy()
    tb_multi_without_regions = tb_multi.drop(columns=columns_multi).copy()

    ## Get columns from tb_*_avg tables relevant
    tb_countries_avg_multi = tb_countries_avg.loc[:, cols_index + columns_multi].dropna(subset=columns_multi, how="all")
    tb_population_avg_multi = tb_population_avg.loc[:, cols_index + columns_multi].dropna(
        subset=columns_multi, how="all"
    )

    ## Add suffix (population-weighted) where applicable
    tb_population_avg_multi["country"] = tb_population_avg_multi["country"] + " (population-weighted)"

    ## Concatenate and create tb_uni_with_regions
    tb_multi_with_regions = concat(
        [
            tb_multi_with_regions,
            tb_countries_avg_multi,
            tb_population_avg_multi,
        ],
        ignore_index=True,
    )
    return tb_uni_without_regions, tb_uni_with_regions, tb_multi_without_regions, tb_multi_with_regions


def _split_into_uni_and_multi(tb: Table) -> tuple[Table, Table]:
    """Split indicators into unidimensional vs multidimensional tables.

    Separates democracy indicators based on whether they have confidence intervals.
    Multidimensional indicators have "_low" and "_high" variants representing
    uncertainty bounds around the main estimate.

    Args:
        tb: V-Dem data with mixed indicator types

    Returns:
        Tuple of (unidimensional_table, multidimensional_table)

    Example:
        Unidimensional: electoff_vdem (single score)
        Multidimensional: civ_libs_vdem, civ_libs_vdem_low, civ_libs_vdem_high
    """
    # Get list of indicators with multi-dimensions (and with one dimension)
    index = ["country", "year"]
    indicators_multi = [col for col in tb.columns if "_low" in col]
    indicators_multi = (
        indicators_multi
        + [i.replace("_low", "_high") for i in indicators_multi]
        + [i.replace("_low", "") for i in indicators_multi]
        + ["wom_parl_vdem"]
    )
    indicators_uni = [col for col in tb.columns if col not in indicators_multi + index]

    # Create two tables
    tb_multi = tb.loc[:, index + indicators_multi].copy()
    tb_uni = tb.loc[:, index + indicators_uni].copy()

    # Reformat multi-dimensions indicators
    tb_multi = from_wide_to_long(
        tb_multi,
        indicator_name_callback=lambda x: x.replace("_low", "").replace("_high", ""),
        indicator_category_callback=lambda x: "low" if "_low" in x else "high" if "_high" in x else "best",
        column_dimension_name="estimate",
    )
    # Set dtypes
    col_ints = [
        "transplaws_row",
        "lib_dich_row",
        "electmulpar_row",
        "electmulpar_leg_row",
        "electmulpar_hoe_row_owid",
        "electfreefair_row",
        "electdem_dich_row_owid",
        "accessjust_m_row",
        "accessjust_w_row",
    ]
    col_ints = [col for col in col_ints if col in tb_multi.columns]
    col_float = [col for col in tb_multi.columns if col not in col_ints + ["country", "year", "estimate"]]
    tb_multi[col_ints] = tb_multi[col_ints].astype("Int32")
    tb_multi[col_float] = tb_multi[col_float].astype("Float32")

    return tb_uni, tb_multi


def _add_note_on_region_averages(tb: Table) -> Table:
    """Add explanatory note about regional averaging methodology.

    Appends a description to indicator metadata explaining that regional values
    are calculated by averaging country-level values within each region.

    Args:
        tb: Table with regional averages

    Returns:
        Table with updated metadata descriptions
    """
    note = "We have estimated the values for regions by averaging the values from the countries in the region."
    cols_indicators = [col for col in tb.columns if col in INDICATORS_REGION_AVERAGES]
    for col in cols_indicators:
        if tb[col].metadata.description_processing:
            tb[col].metadata.description_processing += f"\n\n{note}"
        else:
            tb[col].metadata.description_processing = f"{note}"
    return tb


# %% OTHERS
def add_regions_and_global_aggregates(
    tb: Table,
    ds_regions: Dataset,
    aggregations: dict[str, str] | None = None,
    min_num_values_per_year: int | None = None,
    aggregations_world: dict[str, str] | None = None,
    countries_must_have_data: dict[str, list[str]] | None = None,
    frac_must_have_data: dict[str, float] | None = None,
) -> Table:
    """Add regional and global aggregates to country-level data.

    Calculates regional aggregates using specified aggregation methods
    (sum, mean, etc.) and adds a global "World" aggregate.

    Args:
        tb: Country-level data table
        ds_regions: Regional classification dataset
        aggregations: Method for regional aggregation (default: sum)
        min_num_values_per_year: Minimum countries needed for regional estimate
        aggregations_world: Method for world aggregation (default: sum)

    Returns:
        Table with only regional and world aggregates (country data removed)

    Example:
        Input: Country data for Germany, France, Italy...
        Output: Regional data for Europe, World
    """
    # Estimate region aggregates
    tb_regions = geo.add_regions_to_table(
        tb.copy(),
        ds_regions,
        regions=REGIONS,
        aggregations=aggregations,
        min_num_values_per_year=min_num_values_per_year,
        countries_that_must_have_data=countries_must_have_data,
        frac_countries_that_must_have_data=frac_must_have_data,
    )
    tb_regions = tb_regions.loc[tb_regions["country"].isin(REGIONS.keys())]

    # Add world
    if aggregations_world is None:
        tb_world = tb.groupby("year", as_index=False).sum(numeric_only=True, min_count=1).assign(country="World")
    else:
        tb_world = tb.groupby("year", as_index=False).agg(aggregations_world).assign(country="World")
    tb = concat([tb_regions, tb_world], ignore_index=True, short_name="region_counts")

    return tb


def make_table_with_dummies(tb: Table, people_living_in: bool = False) -> Table:
    """Convert categorical indicators to dummy variables for aggregation.

    Transforms categorical variables (like regime types) into binary dummy variables
    to enable counting how many countries fall into each category.

    Args:
        tb: V-Dem data with categorical indicators
        people_living_in: True if we are counting people instead of countries. In that case, the time-serie might contain additional rows (NA most likely) after expanding observations.

    Returns:
        Table with binary dummy variables for each category

    Example:
        Input: regime_type = ["democracy", "autocracy", "democracy"]
        Output: regime_democracy = [1, 0, 1], regime_autocracy = [0, 1, 0]

    Note:
        Missing values are coded as "-1" category to track data availability.
    """
    tb_ = tb.copy()

    # Define indicators for which we will create dummies
    indicators = [
        {
            "name": "regime_row_owid",
            "name_new": "num_countries_regime",
            # "values_expected": set(map(str, range(4))),
            "values_expected": {
                "0": "closed autocracy",
                "1": "electoral autocracy",
                "2": "electoral democracy",
                "3": "liberal democracy",
            },
            "has_na": True,
        },
        {
            "name": "regime_amb_row_owid",
            "name_new": "num_countries_regime_amb",
            # "values_expected": set(map(str, range(10))),
            "values_expected": {
                "0": "closed autocracy",
                "1": "closed (maybe electoral) autocracy",
                "2": "electoral (maybe closed) autocracy",
                "3": "electoral autocracy",
                "4": "electoral autocracy (maybe electoral democracy)",
                "5": "electoral democracy (maybe electoral autocracy)",
                "6": "electoral democracy",
                "7": "electoral democracy (maybe liberal democracy)",
                "8": "liberal democracy (maybe electoral democracy)",
                "9": "liberal democracy",
            },
            "has_na": True,
        },
        {
            "name": "num_years_in_electdem_consecutive_cat",
            "name_new": "num_countries_years_in_electdem",
            "values_expected": {
                "closed autocracy",
                "electoral autocracy",
                "1-18 years",
                "19-30 years",
                "31-60 years",
                "61-90 years",
                "91+ years",
            },
            "has_na": True,
        },
        {
            "name": "num_years_in_libdem_consecutive_cat",
            "name_new": "num_countries_years_in_libdem",
            "values_expected": {
                "closed autocracy",
                "electoral autocracy",
                "electoral democracy",
                "1-18 years",
                "19-30 years",
                "31-60 years",
                "61-90 years",
                "91+ years",
            },
            "has_na": True,
        },
        {
            "name": "wom_parl_vdem_cat",
            "name_new": "num_countries_wom_parl",
            "values_expected": {
                "0% women",
                "0-10% women",
                "10-20% women",
                "20-30% women",
                "30-40% women",
                "40-50% women",
                "50%+ women",
            },
            "has_na": True,
        },
        {
            "name": "wom_hog_vdem",
            "name_new": "num_countries_hog",
            "values_expected": {
                "0": "Man",
                "1": "Woman",
            },
            "has_na": True,
        },
        {
            "name": "wom_hos_vdem",
            "name_new": "num_countries_hos",
            "values_expected": {
                "0": "Man",
                "1": "Woman",
            },
            "has_na": True,
        },
        {
            "name": "wom_hoe_vdem",
            "name_new": "num_countries_hoe",
            "values_expected": {
                "0": "Man",
                "1": "Woman",
            },
            "has_na": True,
        },
        {
            "name": "held_national_election",
            "name_new": "num_countries_natelect",
            "values_expected": {
                "0": "didn't hold a national election",
                "1": "held a national election",
            },
            "has_na": False,
            "has_na_once_expanded": True,
        },
        {
            "name": "wom_hoe_ever",
            "name_new": "num_countries_wom_hoe_ever",
            "values_expected": {
                "0": "no",
                "1": "yes",
            },
            "has_na": True,
            "has_na_once_expanded": True,
        },
        {
            "name": "wom_hoe_ever_dem",
            "name_new": "num_countries_wom_hoe_ever_demelect",
            "values_expected": {
                "0": "no",
                "1": "yes",
            },
            "has_na": True,
            "has_na_once_expanded": True,
        },
    ]

    # Convert to string
    indicator_names = [indicator["name"] for indicator in indicators]
    tb_[indicator_names] = tb_[indicator_names].astype("string")

    # Sanity check that the categories for each indicator are as expected
    for indicator in indicators:
        values_expected = indicator["values_expected"]
        # Check and fix NA (convert NAs to -1 category)
        ## Should use one flag or another depending on whether we are counting people or countries
        ## If counting people (people_living_in=True), we should use `has_na_once_expanded` flag, otherwise `has_na`
        ## Else, if we are counting countries, we should use `has_na` flag
        if people_living_in:
            has_na = indicator.get("has_na_once_expanded", indicator["has_na"])
        else:
            has_na = indicator["has_na"]

        if has_na:
            # Assert that there are actually NaNs
            assert tb_[indicator["name"]].isna().any(), f"No NA found for indicator {indicator['name']}!"
            # If NA, we should not have category '-1', otherwise these would get merged!
            assert "-1" not in set(
                tb_[indicator["name"]].unique()
            ), f"Error for indicator `{indicator['name']}`. Found -1, which is not allowed when `has_na=True`!"
            tb_[indicator["name"]] = tb_[indicator["name"]].fillna("-1")
            # Add '-1' as a possible category
            if isinstance(values_expected, dict):
                indicator["values_expected"]["-1"] = "-1"
            else:
                values_expected |= {"-1"}
        else:
            assert not tb_[indicator["name"]].isna().any(), f"NA found for {indicator['name']}!"

        values_found = set(tb_[indicator["name"]].unique())
        assert values_found == set(
            values_expected
        ), f"Error for indicator `{indicator['name']}`. Expected {set(values_expected)} but found {values_found}"

        # Rename dimension values
        if isinstance(values_expected, dict):
            tb_[indicator["name"]] = tb_[indicator["name"]].map(indicator["values_expected"])

    ## Rename columns
    tb_ = tb_.rename(columns={indicator["name"]: indicator["name_new"] for indicator in indicators})
    indicator_names = [indicator["name_new"] for indicator in indicators]

    ## Get dummy indicator table
    tb_ = cast(Table, pd.get_dummies(tb_, dummy_na=True, columns=indicator_names, dtype=int))

    ## Add missing metadata to dummy indicators
    dummy_cols = []
    for indicator in indicators:
        ## get list of dummy indicator column names
        if isinstance(indicator["values_expected"], dict):
            dummy_columns = [f"{indicator['name_new']}_{v}" for v in indicator["values_expected"].values()]
        else:
            dummy_columns = [f"{indicator['name_new']}_{v}" for v in indicator["values_expected"]]
        ## assign metadata to dummy column indicators
        for col in dummy_columns:
            tb_[col].metadata = tb[indicator["name"]].metadata
        dummy_cols.extend(dummy_columns)

    ### Select subset of columns
    tb_ = tb_.loc[:, ["year", "country"] + dummy_cols]

    return tb_
