"""Load a meadow dataset and create a garden dataset."""

from typing import Tuple, cast

import numpy as np
from owid.catalog import Dataset, Table
from owid.catalog.tables import concat
from shared import (
    add_population_in_dummies,
    add_regions_and_global_aggregates,
    expand_observations,
    from_wide_to_long,
    make_table_with_dummies,
)

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Missing classifications of states
REGIONS = {
    "Africa": {},
    "Asia": {},
    "North America": {},
    "South America": {},
    "Europe": {},
    "Oceania": {},
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("bti")
    ds_regions = paths.load_dataset("regions")
    ds_population = paths.load_dataset("population")

    # Read table from meadow dataset.
    tb = ds_meadow["bti"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )

    # Invert the scores of regime_bti
    tb["regime_bti"] = 5 - (tb["regime_bti"] - 1)

    # Sanity checks
    tb = check_pol_sys(tb)
    tb = check_regime(tb)
    tb = tb.drop(
        columns=[
            "pol_sys",
        ]
    )

    ##################################################
    # AGGREGATES

    # Get country-count-related data: country-averages, number of countries, ...
    tb_num_countries, tb_avg_countries = get_country_data(tb, ds_regions)

    # Get population-related data: population-weighed averages, people livin in ...
    tb_num_people, tb_avg_w_countries = get_population_data(tb, ds_regions, ds_population)
    ##################################################

    # Add regions to main table
    tb = concat([tb, tb_avg_countries], ignore_index=True)

    #
    # Save outputs.
    #
    tables = [
        tb.format(["country", "year"]),
        tb_num_countries.format(["country", "year", "category"], short_name="num_countries"),
        tb_num_people.format(["country", "year", "category"], short_name="num_people"),
        tb_avg_w_countries.format(["country", "year"], short_name="avg_pop"),
    ]

    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=tables, check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def check_pol_sys(tb: Table) -> Table:
    """Sanity-check the indicator.

    Some years looked off in the previous edition, this is a sanity check.
    """
    col_tmp = "pol_sys_check"

    tb.loc[
        (tb["electfreefair_bti"] >= 6)
        & (tb["electfreefair_bti"].notna())
        & (tb["effective_power_bti"] >= 4)
        & (tb["effective_power_bti"].notna())
        & (tb["freeassoc_bti"] >= 4)
        & (tb["freeassoc_bti"].notna())
        & (tb["freeexpr_bti"] >= 4)
        & (tb["freeexpr_bti"].notna())
        & (tb["sep_power_bti"] >= 4)
        & (tb["sep_power_bti"].notna())
        & (tb["civ_rights_bti"] >= 4)
        & (tb["civ_rights_bti"].notna())
        & (tb["state_basic_bti"] >= 3)
        & (tb["state_basic_bti"].notna()),
        col_tmp,
    ] = 1

    # Replace pol_sys_check = 0 if any condition is not met
    tb.loc[
        (tb["electfreefair_bti"] < 6)
        | (tb["effective_power_bti"] < 4)
        | (tb["freeassoc_bti"] < 4)
        | (tb["freeexpr_bti"] < 4)
        | (tb["sep_power_bti"] < 4)
        | (tb["civ_rights_bti"] < 4)
        | (tb["state_basic_bti"] < 3),
        col_tmp,
    ] = 0

    # print(tb[["pol_sys", "pol_sys_check"]].dropna().value_counts())

    assert (tb["pol_sys"] == tb[col_tmp]).all(), "Miss-labelled `pol_sys`."

    tb = tb.drop(columns=[col_tmp])

    return tb


def check_regime(tb: Table) -> Table:
    col_tmp = "regime_bti_check"
    tb.loc[(tb["pol_sys"] == 0) & (tb["democracy_bti"] >= 1) & (tb["democracy_bti"] < 4), col_tmp] = 1
    tb.loc[(tb["pol_sys"] == 0) & (tb["democracy_bti"] >= 4) & (tb["democracy_bti"] <= 10), col_tmp] = 2
    tb.loc[(tb["pol_sys"] == 1) & (tb["democracy_bti"] >= 1) & (tb["democracy_bti"] < 6), col_tmp] = 3
    tb.loc[(tb["pol_sys"] == 1) & (tb["democracy_bti"] >= 6) & (tb["democracy_bti"] < 8), col_tmp] = 4
    tb.loc[(tb["pol_sys"] == 1) & (tb["democracy_bti"] >= 8) & (tb["democracy_bti"] <= 10), col_tmp] = 5

    tb[col_tmp] = tb[col_tmp].astype("UInt8")

    assert (tb["regime_bti"] == tb[col_tmp]).all(), "Miss-labelled `regime_bti`."

    tb = tb.drop(columns=[col_tmp])
    return tb


def get_country_data(tb: Table, ds_regions: Dataset) -> Tuple[Table, Table]:
    """Estimate number of countries in each regime, and country-average for some indicators.

    Returns two tables:

    1) tb_num_countres: Counts countries in different regimes
        regime_bti (counts)
            - Number of hard-line autocracies
            - Number of moderate autocracies
            - Number of highly defective democracies
            - Number of defective democracies
            - Number of consolidating democracies

    2) tb_avg_countries: Country-average for some indicators
        - democracy_bti (country-average)

    """
    # 1/ COUNT COUNTRIES
    # Keep only non-imputed data
    tb_num = tb.copy()

    # Set INTs
    tb_num = tb_num.astype(
        {
            "regime_bti": "Int64",
        }
    )
    tb_num = cast(Table, tb_num)

    # Define columns on which we will estimate (i) "number of countries" and (ii) "number of people living in ..."
    indicators = [
        {
            "name": "regime_bti",
            "name_new": "num_regime_bti",
            "values_expected": {
                "1": "hard-line autocracy",
                "2": "moderate autocracy",
                "3": "highly defective democracy",
                "4": "defective democracy",
                "5": "consolidating democracy",
            },
            "has_na": True,
        },
    ]

    # Column per indicator-dimension
    tb_num = make_table_with_dummies(tb_num, indicators)

    # Add regions and global aggregates
    tb_num = add_regions_and_global_aggregates(tb_num, ds_regions)
    tb_num = from_wide_to_long(tb_num)

    # 2/ COUNTRY-AVERAGE INDICATORS
    tb_avg = tb.copy()
    indicators_avg = ["democracy_bti"]

    # Keep only relevant columns
    tb_avg = tb_avg.loc[:, ["year", "country"] + indicators_avg]

    # Estimate region aggregates
    tb_avg = add_regions_and_global_aggregates(
        tb=tb_avg,
        ds_regions=ds_regions,
        aggregations={k: "mean" for k in indicators_avg},  # type: ignore
        aggregations_world={k: np.mean for k in indicators_avg},  # type: ignore
    )

    # Keep only certain year range
    # tb_avg = tb_avg.loc[tb_avg["year"].between(YEAR_AGG_MIN, YEAR_AGG_MAX)]

    return tb_num, tb_avg


def get_population_data(tb: Table, ds_regions: Dataset, ds_population: Dataset) -> Tuple[Table, Table]:
    """Estimate people living in each regime, and population-weighted averages for some indicators.

    1) tb_num_people: People living in different regimes
        regime_bti
            - Number of hard-line autocracies
            - Number of moderate autocracies
            - Number of highly defective democracies
            - Number of defective democracies
            - Number of consolidating democracies

    2) tb_avg_w_countries: Population-weighted-average for some indicators
        - democracy_bti

    """
    # 1/ COUNT PEOPLE
    # Keep only non-imputed data
    tb_ppl = tb.copy()

    # Set INTs
    tb_ppl = tb_ppl.astype(
        {
            "regime_bti": "Int64",
        }
    )
    tb_ppl = cast(Table, tb_ppl)

    indicators = [
        {
            "name": "regime_bti",
            "name_new": "num_regime_bti",
            "values_expected": {
                "1": "hard-line autocracy",
                "2": "moderate autocracy",
                "3": "highly defective democracy",
                "4": "defective democracy",
                "5": "consolidating democracy",
            },
            "has_na": True,
        },
    ]

    ## Get missing years (not to miss anyone!) -- Note that this can lead to country overlaps (e.g. USSR and Latvia)
    tb_ppl = expand_observations_without_duplicates(tb_ppl)
    print(f"{tb.shape} -> {tb_ppl.shape}")

    # Column per indicator-dimension
    tb_ppl = make_table_with_dummies(tb_ppl, indicators)

    # Replace USSR -> current states
    # tb_ppl = replace_ussr(tb_ppl, ds_regions)

    ## Counts
    tb_ppl = add_population_in_dummies(tb_ppl, ds_population)
    tb_ppl = add_regions_and_global_aggregates(tb_ppl, ds_regions)
    tb_ppl = from_wide_to_long(tb_ppl)

    # 2/ COUNTRY-AVERAGE INDICATORS
    tb_avg = tb.copy()
    indicators_avg = ["democracy_bti"]

    # Keep only relevant columns
    tb_avg = tb_avg.loc[:, ["year", "country"] + indicators_avg]

    # Add population in dummies (population value replaces 1, 0 otherwise)
    tb_avg = add_population_in_dummies(
        tb_avg,
        ds_population,
        drop_population=False,
    )

    # Get region aggregates
    tb_avg = add_regions_and_global_aggregates(
        tb=tb_avg,
        ds_regions=ds_regions,
        aggregations={k: "sum" for k in indicators_avg} | {"population": "sum"},  # type: ignore
        min_num_values_per_year=1,
    )

    # Normalize by region's population
    columns_index = ["year", "country"]
    columns_indicators = [col for col in tb_avg.columns if col not in columns_index + ["population"]]
    tb_avg[columns_indicators] = tb_avg[columns_indicators].div(tb_avg["population"], axis=0)
    tb_avg = tb_avg.drop(columns="population")

    # Keep only certain year range
    # tb_avg = tb_avg.loc[tb_avg["year"].between(YEAR_AGG_MIN, YEAR_AGG_MAX)]

    tb_avg = tb_avg.rename(
        columns={
            "democracy_bti": "democracy_bti_weighted",
        }
    )
    return tb_ppl, tb_avg


def expand_observations_without_duplicates(tb: Table) -> Table:
    # Full expansion
    tb_exp = expand_observations(tb)

    # Limit years
    # tb_exp = tb_exp[tb_exp["year"].between(YEAR_AGG_MIN, YEAR_AGG_MAX)]

    # # Limit entries to avoid duplicates
    # tb_exp = tb_exp.loc[
    #     ~(
    #         # YUGOSLAVIA
    #         ((tb_exp["country"] == "Yugoslavia") & ((tb_exp["year"] > 1990) | (tb_exp["year"] < 1921)))
    #         | ((tb_exp["country"] == "Slovenia") & ((tb_exp["year"] >= 1921) & (tb_exp["year"] <= 1990)))
    #         | ((tb_exp["country"] == "North Macedonia") & ((tb_exp["year"] >= 1921) & (tb_exp["year"] <= 1990)))
    #         | ((tb_exp["country"] == "Croatia") & ((tb_exp["year"] >= 1921) & (tb_exp["year"] <= 1990)))
    #         | ((tb_exp["country"] == "Serbia and Montenegro") & ((tb_exp["year"] > 2005) | (tb_exp["year"] <= 1990)))
    #         ## YUG 2
    #         | ((tb_exp["country"] == "Bosnia and Herzegovina") & ((tb_exp["year"] >= 1921) & (tb_exp["year"] <= 1991)))
    #         | ((tb_exp["country"] == "Serbia") & ((tb_exp["year"] >= 1921) & (tb_exp["year"] <= 2005)))
    #         | ((tb_exp["country"] == "Montenegro") & ((tb_exp["year"] >= 1921) & (tb_exp["year"] <= 2005)))
    #         | ((tb_exp["country"] == "Kosovo") & ((tb_exp["year"] >= 1921) & (tb_exp["year"] <= 2007)))
    #         # YEMEN
    #         | ((tb_exp["country"] == "Yemen Arab Republic") & ((tb_exp["year"] > 1989) | (tb_exp["year"] < 1940)))
    #         | ((tb_exp["country"] == "Yemen People's Republic") & ((tb_exp["year"] > 1989) | (tb_exp["year"] < 1940)))
    #         | ((tb_exp["country"] == "Yemen") & ((tb_exp["year"] >= 1940) & (tb_exp["year"] <= 1989)))
    #         # GERMANY
    #         | ((tb_exp["country"] == "West Germany") & ((tb_exp["year"] > 1989) | (tb_exp["year"] < 1949)))
    #         | ((tb_exp["country"] == "East Germany") & ((tb_exp["year"] > 1989) | (tb_exp["year"] < 1949)))
    #         | ((tb_exp["country"] == "Germany") & (tb_exp["year"] >= 1949) & (tb_exp["year"] <= 1989))
    #         # USSR
    #         | ((tb_exp["country"] == "USSR") & ((tb_exp["year"] > 1991) | (tb_exp["year"] < 1941)))
    #         | ((tb_exp["country"] == "Uzbekistan") & (tb_exp["year"] >= 1941) & (tb_exp["year"] <= 1991))
    #         | ((tb_exp["country"] == "Kazakhstan") & (tb_exp["year"] >= 1941) & (tb_exp["year"] <= 1991))
    #         | ((tb_exp["country"] == "Turkmenistan") & (tb_exp["year"] >= 1941) & (tb_exp["year"] <= 1991))
    #         | ((tb_exp["country"] == "Kyrgyzstan") & (tb_exp["year"] >= 1941) & (tb_exp["year"] <= 1991))
    #         | ((tb_exp["country"] == "Tajikistan") & (tb_exp["year"] >= 1941) & (tb_exp["year"] <= 1991))
    #         | ((tb_exp["country"] == "Russia") & (tb_exp["year"] >= 1941) & (tb_exp["year"] <= 1991))
    #         | ((tb_exp["country"] == "Ukraine") & (tb_exp["year"] >= 1941) & (tb_exp["year"] <= 1991))
    #         | ((tb_exp["country"] == "Belarus") & (tb_exp["year"] >= 1941) & (tb_exp["year"] <= 1991))
    #         | ((tb_exp["country"] == "Moldova") & (tb_exp["year"] >= 1941) & (tb_exp["year"] <= 1991))
    #         | ((tb_exp["country"] == "Latvia") & (tb_exp["year"] >= 1941) & (tb_exp["year"] <= 1991))
    #         | ((tb_exp["country"] == "Lithuania") & (tb_exp["year"] >= 1941) & (tb_exp["year"] <= 1991))
    #         | ((tb_exp["country"] == "Estonia") & (tb_exp["year"] >= 1941) & (tb_exp["year"] <= 1991))
    #         | ((tb_exp["country"] == "Armenia") & (tb_exp["year"] >= 1941) & (tb_exp["year"] <= 1991))
    #         | ((tb_exp["country"] == "Georgia") & (tb_exp["year"] >= 1941) & (tb_exp["year"] <= 1991))
    #         | ((tb_exp["country"] == "Azerbaijan") & (tb_exp["year"] >= 1941) & (tb_exp["year"] <= 1991))
    #         # CZECHOSLOVAKIA
    #         | ((tb_exp["country"] == "Czechoslovakia") & ((tb_exp["year"] > 1992) | (tb_exp["year"] < 1918)))
    #         | ((tb_exp["country"] == "Czechia") & ((tb_exp["year"] <= 1992) & (tb_exp["year"] >= 1918)))
    #         | ((tb_exp["country"] == "Slovakia") & ((tb_exp["year"] <= 1992) & (tb_exp["year"] >= 1918)))
    #     ),
    # ]

    return tb_exp
