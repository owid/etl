"""Load a meadow dataset and create a garden dataset.

LABELS

`exec_reccomp_polity`
    0 "power seized"
    1 "elite selection"
    2 "dual or transitional"
    3 "election"

`exec_recopen_polity`
    0 "power seized"
    1 "hereitary succession"
    2 "dual, chief minister designated"
    3 "dual, chief minister elected"
    4 "open"

`exec_constr_polity`
    1 "unconstrained"
    2 ?
    3 "slight to moderate"
    4 ?
    5 "substantial"
    6 ?
    7 "executive parity or subordination"

`polpart_reg_polity`
    1 "unregulated"
    2 "multiple identities"
    3 "sectarian"
    4 "restricted"
    5 "unrestricted and stable"

`polpart_comp_polity`
    0 "unregulated"
    1 "repressed"
    2 "suppressed"
    3 "factional"
    4 "transitional"
    5 "competitive"
"""

import ast
from typing import Tuple, cast

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.tables import concat
from shared import (
    add_age_groups,
    add_count_years_in_regime,
    add_imputes,
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
PATH_IMPUTE = paths.directory / f"{paths.short_name}.countries_impute.yml"


# Regime labels
REGIME_LABELS = {
    0: "autocracy",
    1: "anocracy",
}

# Missing classifications of states
REGIONS = {
    "Africa": {},
    "Asia": {},
    "North America": {
        "additional_members": [
            "United Provinces of Central America",
        ]
    },
    "South America": {},
    "Europe": {
        "additional_members": [
            "Prussia",
        ]
    },
    "Oceania": {},
}

# Year range for aggregates
YEAR_AGG_MIN = 1800
YEAR_AGG_MAX = 2018


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("polity")
    ds_regions = paths.load_dataset(short_name="regions")
    ds_population = paths.load_dataset(short_name="population")

    # Read table from meadow dataset.
    tb = ds_meadow["polity"].reset_index()

    #
    # Process data.
    #
    # Column rename
    columns_rename = {
        "polity2": "democracy_polity",
        "xrcomp": "exec_reccomp_polity",
        "xropen": "exec_recopen_polity",
        "xconst": "exec_constr_polity",
        "parreg": "polpart_reg_polity",
        "parcomp": "polpart_comp_polity",
    }
    tb = tb.rename(columns=columns_rename)

    # Assign NaNs to categories -66, -77 and -88
    cols = list(columns_rename.values())
    tb[cols] = tb[cols].replace([-66, -77, -88], pd.NA).astype("Int64")

    # Generate regime variables as per the (conventional) rules here: https://www.systemicpeace.org/polityproject.html
    tb = add_regime_category(tb)

    # Harmonize country names
    tb = harmonize_country_names(tb)

    # Recode
    tb["democracy_recod_polity"] = tb["democracy_polity"] + 10

    # Age and experience of democracy
    tb = add_age_and_experience(tb)

    # Add imputes
    col_flag_imputed = "values_imputed"
    tb = add_imputes(tb=tb, path=PATH_IMPUTE, col_flag_imputed=col_flag_imputed)

    # Remove countries to avoid overlaps
    # tb = tb.loc[~((tb["country"] == "USSR") & (tb["year"] == 1991))]

    ##################################################
    # AGGREGATES

    # Get country-count-related data: country-averages, number of countries, ...
    tb_num_countries, tb_avg_countries = get_country_data(tb, ds_regions)

    # Get population-related data: population-weighed averages, people livin in ...
    tb_num_people, tb_avg_w_countries = get_population_data(tb, ds_regions, ds_population)

    # Get region data
    # tb_regions = tb.loc[~tb[col_flag_imputed]].drop(columns=[col_flag_imputed]).copy()

    ##################################################

    # Add regions to main table
    tb = concat([tb, tb_avg_countries], ignore_index=True)

    # Remove columns
    tb = tb.drop(columns=[col_flag_imputed, "ccode"])

    # Format table
    tb = tb.format(["country", "year"])
    tb_num_countries = tb_num_countries.format(["country", "year", "category"], short_name="num_countries")
    tb_num_people = tb_num_people.format(["country", "year", "category"], short_name="num_people")
    tb_avg_w_countries = tb_avg_w_countries.format(["country", "year"], short_name="avg_pop")

    #
    # Save outputs.
    #
    tables = [
        tb,
        tb_num_countries,
        tb_num_people,
        tb_avg_w_countries,
    ]

    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=tables, check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_regime_category(tb: Table) -> Table:
    """Estimate indicator for regime type based on `democracy_polity` categories."""
    series = pd.cut(tb["democracy_polity"], [-10.1, -6, 5, 10], labels=[0, 1, 2])
    tb["regime_polity"] = series.copy_metadata(tb["democracy_polity"])
    return tb


def harmonize_country_names(tb: Table) -> Table:
    """Harmonize country names, including former state corrections."""
    ## Fix Pakistan entity (ccode = 769 is actually 'Pakistan (former)')
    tb["country"] = tb["country"].astype("string")
    tb.loc[tb["ccode"] == 769, "country"] = "Pakistan (former)"

    ## Fix Sudan: remove former country for 2011 (already have north/south sudan data that year)
    tb = tb.loc[~((tb["ccode"] == 625) & (tb["year"] == 2011))]

    ## Fix Serbia and Montenegro: remove data for Serbia@2006 (already have S&M data that year)
    # tb = tb.loc[~((tb["ccode"] == 342) & (tb["year"] == 2006))]

    ## Fix Ethiopia (former): remove that @1993 (already have ethiopia/eritrea)
    tb = tb.loc[~((tb["ccode"] == 530) & (tb["year"] == 1993))]

    ## Classic harmization
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )

    # Sanity check: Only one ccode for country
    assert tb.groupby("country")["ccode"].nunique().max() == 1, "Multiple ccode for country"

    return tb


def add_age_and_experience(tb: Table) -> Table:
    """Add age and experience related indicators.

    This includes:
        - Number of consecutive years in electoral democracy and polyarchy (age)
        - Number of total years in electoral democracy and polyarchy (experience)
        - Age groups for electoral democracy and polyarchy
    """
    columns = [
        ("regime_polity", "dem_polity", 1),
        # ("regime_lied", "polyarchy_lied", 6),
    ]
    # Add age and experience counts
    tb = add_count_years_in_regime(
        tb=tb,
        columns=columns,  # type: ignore
        na_is_zero=True,
    )

    for col in columns:
        col_age = f"age_{col[1]}"

        # Add age groups
        tb = add_age_groups(tb=tb, column=col_age, column_raw=col[0], category_names=REGIME_LABELS, threshold=col[2])

        # Replace category numbers with labels (age in *)
        mapping = {num: label for num, label in REGIME_LABELS.items() if num <= col[2]}
        mask = (tb[col_age] == 0) | (tb[col_age].isna())
        tb.loc[mask, col_age] = tb.loc[mask, col[0]].replace(mapping)
        tb[col_age] = tb[col_age].astype("string")

    return tb


def get_country_data(tb: Table, ds_regions: Dataset) -> Tuple[Table, Table]:
    """Estimate number of countries in each regime, and country-average for some indicators.

    Returns two tables:

    1) tb_num_countres: Counts countries in different categories
        regime_polity (counts)
            - Number of autocracies
            - Number of anocracies
            - Number of democracies

        group_age_dem_polity (counts)
            - Number of democracies aged 1-18 years
            - Number of democracies aged 19-30 years
            - Number of democracies aged 31-60 years
            - Number of democracies aged 61-90 years
            - Number of democracies aged 91+ years

    2) tb_avg_countries: Country-average for some indicators
        - democracy_polity (country-average)
        - democracy_recod_polity (country-average)
    """
    tb_ = tb.loc[~tb["values_imputed"]].copy()

    # 1/ COUNT COUNTRIES
    # Keep only non-imputed data
    tb_num = tb_.copy()

    # Set INTs
    tb_num = tb_num.astype(
        {
            "regime_polity": "Int64",
        }
    )
    tb_num = cast(Table, tb_num)

    # Define columns on which we will estimate (i) "number of countries" and (ii) "number of people living in ..."
    indicators = [
        {
            "name": "regime_polity",
            "name_new": "num_regime_polity",
            "values_expected": {
                "0": "autocracy",
                "1": "anocracy",
                "2": "democracy",
            },
            "has_na": True,
        },
        {
            "name": "group_age_dem_polity",
            "name_new": "num_group_age_dem_polity",
            "values_expected": {
                "autocracy": "autocracy",
                "anocracy": "anocracy",
                "1-18 years": "1-18 years",
                "19-30 years": "19-30 years",
                "31-60 years": "31-60 years",
                "61-90 years": "61-90 years",
                "91+ years": "91+ years",
            },
            "has_na": True,
        },
    ]

    # Column per indicator-dimension
    tb_num = make_table_with_dummies(tb_num, indicators)

    # Add regions and global aggregates
    tb_num = add_regions_and_global_aggregates(tb_num, ds_regions, regions=REGIONS)
    tb_num = from_wide_to_long(tb_num)

    # Keep only certain year range
    tb_num = tb_num.loc[tb_num["year"].between(YEAR_AGG_MIN, YEAR_AGG_MAX)]

    # 2/ COUNTRY-AVERAGE INDICATORS
    tb_avg = tb_.copy()
    indicators_avg = ["democracy_polity", "democracy_recod_polity"]

    # Keep only relevant columns
    tb_avg = tb_avg.loc[:, ["year", "country"] + indicators_avg]

    # Estimate region aggregates
    tb_avg = add_regions_and_global_aggregates(
        tb=tb_avg,
        ds_regions=ds_regions,
        aggregations={k: "mean" for k in indicators_avg},  # type: ignore
        aggregations_world={k: np.mean for k in indicators_avg},  # type: ignore
        regions=REGIONS,
    )

    # Keep only certain year range
    tb_avg = tb_avg.loc[tb_avg["year"].between(YEAR_AGG_MIN, YEAR_AGG_MAX)]

    return tb_num, tb_avg


def get_population_data(tb: Table, ds_regions: Dataset, ds_population: Dataset) -> Tuple[Table, Table]:
    """Estimate people living in each regime, and population-weighted averages for some indicators.

    regime_polity (people living)
        - People living in autocracies
        - People living in anocracies
        - People living in democracies

    group_age_dem_polity (people living)
        - People living in democracies aged 1-18 years
        - People living in democracies aged 19-30 years
        - People living in democracies aged 31-60 years
        - People living in democracies aged 61-90 years
        - People living in democracies aged 91+ years

    democracy_polity (population-weighed-average)

    """
    tb_ = tb.loc[~tb["values_imputed"]].copy()

    # 1/ COUNT PEOPLE
    # Keep only non-imputed data
    tb_ppl = tb_.copy()

    # Set INTs
    tb_ppl = tb_ppl.astype(
        {
            "regime_polity": "Int64",
        }
    )
    tb_ppl = cast(Table, tb_ppl)

    indicators = [
        {
            "name": "regime_polity",
            "name_new": "pop_regime_polity",
            "values_expected": {
                "0": "autocracy",
                "1": "anocracy",
                "2": "democracy",
            },
            "has_na": True,
        },
        {
            "name": "group_age_dem_polity",
            "name_new": "pop_group_age_dem_polity",
            "values_expected": {
                "autocracy": "autocracy",
                "anocracy": "anocracy",
                "1-18 years": "1-18 years",
                "19-30 years": "19-30 years",
                "31-60 years": "31-60 years",
                "61-90 years": "61-90 years",
                "91+ years": "91+ years",
            },
            "has_na": True,
        },
    ]

    ## Get missing years (not to miss anyone!) -- Note that this can lead to country overlaps (e.g. USSR and Latvia)
    tb_ppl = expand_observations_without_duplicates(tb_ppl)
    print(f"{tb_.shape} -> {tb_ppl.shape}")

    # Column per indicator-dimension
    tb_ppl = make_table_with_dummies(tb_ppl, indicators)

    # Replace USSR -> current states
    tb_ppl = replace_ussr(tb_ppl, ds_regions)

    ## Counts
    tb_ppl = add_population_in_dummies(tb_ppl, ds_population)
    tb_ppl = add_regions_and_global_aggregates(tb_ppl, ds_regions, regions=REGIONS)
    tb_ppl = from_wide_to_long(tb_ppl)

    # 2/ COUNTRY-AVERAGE INDICATORS
    tb_avg = tb_.copy()
    indicators_avg = ["democracy_polity"]

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
    tb_avg = tb_avg.loc[tb_avg["year"].between(YEAR_AGG_MIN, YEAR_AGG_MAX)]

    tb_avg = tb_avg.rename(
        columns={
            "democracy_polity": "democracy_polity_weighted",
        }
    )
    return tb_ppl, tb_avg


def expand_observations_without_duplicates(tb: Table) -> Table:
    # Full expansion
    tb_exp = expand_observations(tb)

    # Limit years
    tb_exp = tb_exp[tb_exp["year"].between(YEAR_AGG_MIN, YEAR_AGG_MAX)]

    # Limit entries to avoid duplicates
    tb_exp = tb_exp.loc[
        ~(
            # YUGOSLAVIA
            ((tb_exp["country"] == "Yugoslavia") & ((tb_exp["year"] > 1990) | (tb_exp["year"] < 1921)))
            | ((tb_exp["country"] == "Slovenia") & ((tb_exp["year"] >= 1918) & (tb_exp["year"] <= 1990)))
            | ((tb_exp["country"] == "North Macedonia") & ((tb_exp["year"] >= 1918) & (tb_exp["year"] <= 1990)))
            | ((tb_exp["country"] == "Croatia") & ((tb_exp["year"] >= 1918) & (tb_exp["year"] <= 1990)))
            | ((tb_exp["country"] == "Serbia and Montenegro") & ((tb_exp["year"] > 2005) | (tb_exp["year"] <= 1990)))
            ## YUG 2
            | ((tb_exp["country"] == "Bosnia and Herzegovina") & ((tb_exp["year"] >= 1921) & (tb_exp["year"] <= 1991)))
            | ((tb_exp["country"] == "Serbia") & ((tb_exp["year"] >= 1921) & (tb_exp["year"] <= 2005)))
            | ((tb_exp["country"] == "Montenegro") & ((tb_exp["year"] >= 1921) & (tb_exp["year"] <= 2005)))
            | ((tb_exp["country"] == "Kosovo") & ((tb_exp["year"] >= 1921) & (tb_exp["year"] <= 2007)))
            # YEMEN
            | ((tb_exp["country"] == "Yemen Arab Republic") & ((tb_exp["year"] > 1989) | (tb_exp["year"] < 1940)))
            | ((tb_exp["country"] == "Yemen People's Republic") & ((tb_exp["year"] > 1989) | (tb_exp["year"] < 1940)))
            | ((tb_exp["country"] == "Yemen") & ((tb_exp["year"] >= 1940) & (tb_exp["year"] <= 1989)))
            # GERMANY
            | ((tb_exp["country"] == "West Germany") & ((tb_exp["year"] > 1989) | (tb_exp["year"] < 1949)))
            | ((tb_exp["country"] == "East Germany") & ((tb_exp["year"] > 1989) | (tb_exp["year"] < 1949)))
            | ((tb_exp["country"] == "Germany") & (tb_exp["year"] >= 1949) & (tb_exp["year"] <= 1989))
            # USSR
            | ((tb_exp["country"] == "USSR") & ((tb_exp["year"] > 1991) | (tb_exp["year"] < 1941)))
            | ((tb_exp["country"] == "Uzbekistan") & (tb_exp["year"] >= 1941) & (tb_exp["year"] <= 1991))
            | ((tb_exp["country"] == "Kazakhstan") & (tb_exp["year"] >= 1941) & (tb_exp["year"] <= 1991))
            | ((tb_exp["country"] == "Turkmenistan") & (tb_exp["year"] >= 1941) & (tb_exp["year"] <= 1991))
            | ((tb_exp["country"] == "Kyrgyzstan") & (tb_exp["year"] >= 1941) & (tb_exp["year"] <= 1991))
            | ((tb_exp["country"] == "Tajikistan") & (tb_exp["year"] >= 1941) & (tb_exp["year"] <= 1991))
            | ((tb_exp["country"] == "Russia") & (tb_exp["year"] >= 1941) & (tb_exp["year"] <= 1991))
            | ((tb_exp["country"] == "Ukraine") & (tb_exp["year"] >= 1941) & (tb_exp["year"] <= 1991))
            | ((tb_exp["country"] == "Belarus") & (tb_exp["year"] >= 1941) & (tb_exp["year"] <= 1991))
            | ((tb_exp["country"] == "Moldova") & (tb_exp["year"] >= 1941) & (tb_exp["year"] <= 1991))
            | ((tb_exp["country"] == "Latvia") & (tb_exp["year"] >= 1941) & (tb_exp["year"] <= 1991))
            | ((tb_exp["country"] == "Lithuania") & (tb_exp["year"] >= 1941) & (tb_exp["year"] <= 1991))
            | ((tb_exp["country"] == "Estonia") & (tb_exp["year"] >= 1941) & (tb_exp["year"] <= 1991))
            | ((tb_exp["country"] == "Armenia") & (tb_exp["year"] >= 1941) & (tb_exp["year"] <= 1991))
            | ((tb_exp["country"] == "Georgia") & (tb_exp["year"] >= 1941) & (tb_exp["year"] <= 1991))
            | ((tb_exp["country"] == "Azerbaijan") & (tb_exp["year"] >= 1941) & (tb_exp["year"] <= 1991))
            # CZECHOSLOVAKIA
            | ((tb_exp["country"] == "Czechoslovakia") & ((tb_exp["year"] > 1992) | (tb_exp["year"] < 1918)))
            | ((tb_exp["country"] == "Czechia") & ((tb_exp["year"] <= 1992) & (tb_exp["year"] >= 1918)))
            | ((tb_exp["country"] == "Slovakia") & ((tb_exp["year"] <= 1992) & (tb_exp["year"] >= 1918)))
        ),
    ]

    return tb_exp


def replace_ussr(tb: Table, ds_regions: Dataset) -> Table:
    tb_regions = ds_regions["regions"]
    codes = tb_regions.loc["OWID_USS", "successors"]
    successors = set(tb_regions.loc[ast.literal_eval(codes), "name"])

    # Create new rows
    tb_succ = []
    for successor in successors:
        # Copy USSR data
        tb_ = tb.loc[(tb["country"] == "USSR")].copy()
        # Replace country name
        tb_["country"] = successor
        # Append
        tb_succ.append(tb_)
    tb_succ = concat(tb_succ, ignore_index=True)

    # Concatenate tables
    tb = concat([tb, tb_succ], ignore_index=True).sort_values(["country", "year"])

    # Remove USSR
    tb = tb.loc[~(tb["country"] == "USSR")]
    return tb

