"""Load a meadow dataset and create a garden dataset."""

from itertools import chain
from typing import Dict, Optional, Tuple, cast

import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.tables import concat
from shared import add_population_in_dummies, expand_observations, from_wide_to_long

from etl.data_helpers import geo
from etl.helpers import PathFinder

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
# Indicators for which we estimate region-averages
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
]
INDICATORS_REGION_AVERAGES = [[f"{ind_name}{dim}" for dim in ["", "_low", "_high"]] for ind_name in _indicators_avg]
INDICATORS_REGION_AVERAGES = list(chain.from_iterable(INDICATORS_REGION_AVERAGES)) + ["wom_parl_vdem"]


def run(tb: Table, ds_regions: Dataset, ds_population: Dataset) -> Tuple[Table, Table, Table, Table, Table, Table]:
    tb_ = tb.copy()

    # Create table with sums and averages
    tb_countries_counts, tb_countries_avg = make_table_countries(tb_, ds_regions)

    # Create table with population-weighted averages
    tb_population_counts, tb_population_avg = make_table_population(tb_, ds_regions, ds_population=ds_population)

    # Consolidate main table with additional regional aggregates
    tb_ = tb_.drop(columns=["regime_imputed_country", "regime_imputed", "histname"])
    tb_uni_without_regions, tb_uni_with_regions, tb_multi_without_regions, tb_multi_with_regions = make_main_tables(
        tb_, tb_countries_avg, tb_population_avg
    )

    # Only have one origin in tb_multi_with_regions
    origin = tb_multi_with_regions["civ_libs_vdem"].m.origins[0]
    assert origin.producer == "V-Dem", "Assigned origin should be V-Dem!"
    for col in tb_multi_with_regions.columns:
        tb_multi_with_regions[col].metadata.origins = [origin]
    # Only have one origin in tb_uni_with_regions
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
def make_table_countries(tb: Table, ds_regions: Dataset) -> Tuple[Table, Table]:
    """Estimate number of countries in X and averages over countries for each region."""
    # Remove imputed countries (they did not exist, so should not count them!)
    tb_ = tb.loc[~tb["regime_imputed"]].copy()

    # Convert country to string
    tb_["country"] = tb_["country"].astype("string")

    # Sanity check: all countries are in the regions
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
    """Get region indicators of type "Number of countries"."""
    tb_ = tb.copy()
    # Generate dummy indicators
    tb_ = make_table_with_dummies(tb_)

    # Estimate region aggregates
    tb_ = add_regions_and_global_aggregates(tb_, ds_regions)

    # Sanity check on output shape
    assert tb_.shape[1] == 52, "Unexpected number of columns."

    # Wide to long format
    tb_ = from_wide_to_long(tb_)

    # Remove some dimensions
    tb_.loc[
        tb_["category"].isin(["closed autocracy", "electoral autocracy", "electoral democracy"]),
        ["num_countries_years_in_electdem", "num_countries_years_in_libdem"],
    ] = float("nan")

    # Remove data pre-1900 for num_countries_wom_parl
    tb_.loc[tb_["year"] < 1900, "num_countries_wom_parl"] = float("nan")

    return tb_


def make_table_countries_avg(tb: Table, ds_regions: Dataset) -> Table:
    """Get region indicators of type "Average of countries"."""
    tb_ = tb.copy()

    # Keep only relevant columns
    cols_indicators = [col for col in tb_.columns if col in INDICATORS_REGION_AVERAGES]
    tb_ = tb_.loc[:, ["year", "country"] + cols_indicators]

    # Estimate region aggregates
    tb_ = add_regions_and_global_aggregates(
        tb=tb_,
        ds_regions=ds_regions,
        aggregations={k: "mean" for k in cols_indicators},  # type: ignore
        aggregations_world={k: "mean" for k in cols_indicators},  # type: ignore
    )

    # Sanity check on output shape
    n_expected = 175
    assert tb_.shape[1] == n_expected, f"Unexpected number of columns. Expected {n_expected} but found {tb_.shape[1]}"

    return tb_


# %% POPULATION TABLES
def make_table_population(tb: Table, ds_regions: Dataset, ds_population: Dataset) -> Tuple[Table, Table]:
    """Estimate number of people in X regime, and averages over countries for each region."""
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
    """Estimate number of people in X regime."""
    tb_ = tb.copy()

    # Get dummy indicators
    tb_ = make_table_with_dummies(tb_)

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
    assert tb_.shape[1] == 52, "Unexpected number of columns."

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
        }
    )

    # Remove some dimensions
    tb_.loc[
        tb_["category"].isin(["closed autocracy", "electoral autocracy", "electoral democracy"]),
        ["population_years_in_electdem", "population_years_in_libdem"],
    ] = float("nan")
    return tb_


def make_table_population_avg(tb: Table, ds_regions: Dataset, ds_population: Dataset) -> Table:
    """Get region/world average estimates on some indicators."""
    tb_ = tb.copy()

    # Keep only relevant columns
    cols_indicators = [col for col in tb_.columns if col in INDICATORS_REGION_AVERAGES]
    tb_ = tb_.loc[:, ["year", "country"] + cols_indicators]

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
        drop_population=False,
    )

    # Get region aggregates
    tb_ = add_regions_and_global_aggregates(
        tb=tb_,
        ds_regions=ds_regions,
        aggregations={k: "sum" for k in cols_indicators} | {"population": "sum"},  # type: ignore
        min_num_values_per_year=1,
    )

    # Normalize by region's population
    columns_index = ["year", "country"]
    columns_indicators = [col for col in tb_.columns if col not in columns_index + ["population"]]
    tb_[columns_indicators] = tb_[columns_indicators].div(tb_["population"], axis=0)
    tb_ = tb_.drop(columns="population")

    # Rename columns
    # tb_ = tb_.rename(columns={col: f"popw_{col}" for col in INDICATORS_REGION_AVERAGES})
    # Sanity check on output shape
    n_expected = 175
    assert tb_.shape[1] == n_expected, f"Unexpected number of columns. Expected {n_expected} but found {tb_.shape[1]}"

    return tb_


def expand_observations_without_leading_to_duplicates(tb: Table) -> Table:
    """Expand observations (accounting for overlaps between former and current countries).

    If the data has data for "USSR" and "Russia" for the same year, we should drop the "USSR" row.
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
def make_main_tables(tb: Table, tb_countries_avg: Table, tb_population_avg: Table) -> Tuple[Table, Table, Table, Table]:
    """Integrate the indicators from region aggregates and add dimensions to indicators.

    This method generates three tables:

        - Unidimensional indicators: Table with uni-dimensional indicators and without regional data.
        - Multidimensional indicators without regions: Table with multi-dimensional indicators and without regional data.
        - Multidimensional indicators with regions: Table with multi-dimensional indicators and with regional data.

        Note: We have estimated regional aggregates with two methods: 'simple mean' or 'population-weighted mean'. We add both flavours, and differentiate them with an additional dimension. (see column `aggregate_method`).
    """
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
    assert set(tb_population_avg) == set(tb_countries_avg), (
        "Columns in tb_population_avg and tb_countries_avg do not match!"
    )

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
    tb_uni_with_regions["aggregate_method"] = "average"
    tb_countries_avg_uni = tb_countries_avg.loc[:, cols_index + columns_uni].dropna(subset=columns_uni, how="all")
    tb_countries_avg_uni["aggregate_method"] = "average"
    tb_population_avg_uni = tb_population_avg.loc[:, cols_index + columns_uni].dropna(subset=columns_uni, how="all")
    tb_population_avg_uni["aggregate_method"] = "population-weighted average"

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
    tb_multi_with_regions["aggregate_method"] = "average"
    tb_countries_avg_multi = tb_countries_avg.loc[:, cols_index + columns_multi].dropna(subset=columns_multi, how="all")
    tb_countries_avg_multi["aggregate_method"] = "average"
    tb_population_avg_multi = tb_population_avg.loc[:, cols_index + columns_multi].dropna(
        subset=columns_multi, how="all"
    )
    tb_population_avg_multi["aggregate_method"] = "population-weighted average"

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


def _split_into_uni_and_multi(tb: Table) -> Tuple[Table, Table]:
    """Split a table into two: one with uni-dimensional indicators, and one with multi-dimensional indicators.

    The table with multi-dimensional indicators will have an additional column (`category`) to differentiate between the different dimension values.
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
    aggregations: Optional[Dict[str, str]] = None,
    min_num_values_per_year: Optional[int] = None,
    aggregations_world: Optional[Dict[str, str]] = None,
) -> Table:
    """Add regions, and world aggregates."""
    tb_regions = geo.add_regions_to_table(
        tb.copy(),
        ds_regions,
        regions=REGIONS,
        aggregations=aggregations,
        min_num_values_per_year=min_num_values_per_year,
    )
    tb_regions = tb_regions.loc[tb_regions["country"].isin(REGIONS.keys())]

    # Add world
    if aggregations_world is None:
        tb_world = tb.groupby("year", as_index=False).sum(numeric_only=True, min_count=1).assign(country="World")
    else:
        tb_world = tb.groupby("year", as_index=False).agg(aggregations_world).assign(country="World")
    tb = concat([tb_regions, tb_world], ignore_index=True, short_name="region_counts")

    return tb


def make_table_with_dummies(tb: Table) -> Table:
    """Format table to have dummy indicators.

    From a table with categorical indicators, create a new table with dummy indicator for each indicator-category pair.

    Example input:

    | year | country |  regime   | regime_amb |
    |------|---------|-----------|------------|
    | 2000 |   USA   |     1     |      0     |
    | 2000 |   CAN   |     0     |      1     |
    | 2000 |   DEU   |    NaN    |      NaN   |


    Example output:

    | year | country | regime_0 | regime_1 | regime_-1 | regime_amb_0 | regime_amb_0 | regime_amb_-1 |
    |------|---------|----------|----------|-----------|--------------|--------------|---------------|
    | 2000 |   USA   |    0     |    1     |     0     |      1       |      0       |       0       |
    | 2000 |   CAN   |    1     |    0     |     0     |      0       |      1       |       0       |
    | 2000 |   DEU   |    0     |    0     |     1     |      0       |      0       |       1       |

    Note that '-1' denotes NA (missing value) category.

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
    ]

    # Convert to string
    indicator_names = [indicator["name"] for indicator in indicators]
    tb_[indicator_names] = tb_[indicator_names].astype("string")

    # Sanity check that the categories for each indicator are as expected
    for indicator in indicators:
        values_expected = indicator["values_expected"]
        # Check and fix NA (convert NAs to -1 category)
        if indicator["has_na"]:
            # Assert that there are actually NaNs
            assert tb_[indicator["name"]].isna().any(), "No NA found!"
            # If NA, we should not have category '-1', otherwise these would get merged!
            assert "-1" not in set(tb_[indicator["name"]].unique()), (
                f"Error for indicator `{indicator['name']}`. Found -1, which is not allowed when `has_na=True`!"
            )
            tb_[indicator["name"]] = tb_[indicator["name"]].fillna("-1")
            # Add '-1' as a possible category
            if isinstance(values_expected, dict):
                indicator["values_expected"]["-1"] = "-1"
            else:
                values_expected |= {"-1"}
        else:
            assert not tb_[indicator["name"]].isna().any(), "NA found!"

        values_found = set(tb_[indicator["name"]].unique())
        assert values_found == set(values_expected), (
            f"Error for indicator `{indicator['name']}`. Expected {set(values_expected)} but found {values_found}"
        )

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
