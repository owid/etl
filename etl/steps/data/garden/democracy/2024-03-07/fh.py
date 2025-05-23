"""Load a meadow dataset and create a garden dataset."""

import ast
from typing import cast

import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.tables import concat
from shared import (
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
PATH_IMPUTE = paths.directory / "fh.countries_impute.yml"
# Minimum dataset year expected
YEAR_MIN = 1972

# Regions
REGIONS = {
    "Africa": {},
    "Asia": {
        "additional_members": [
            "Chechnya",
            "West Bank and Gaza Strip",
            "West Bank",
            "Israeli-Occupied Territories",
            "Palestinian Authority-Administered Territories",
            "Gaza strip",
            "Indian Kashmir",
            "Pakistani Kashmir",
            "Kurdistan",
            "North Vietnam",
            "Tibet",
        ]
    },
    "North America": {},
    "South America": {},
    "Europe": {
        "additional_members": [
            "Crimea",
            "Eastern Donbas",
            "Northern Ireland",
        ]
    },
    "Oceania": {},
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("fh")
    ds_regions = paths.load_dataset("regions")
    ds_population = paths.load_dataset("population")

    # Read table from meadow dataset.
    tb_ratings = ds_meadow["fh_ratings"].reset_index()
    tb_scores = ds_meadow["fh_scores"].reset_index()

    #
    # Process data.
    #
    tb_ratings = geo.harmonize_countries(
        df=tb_ratings,
        countries_file=paths.country_mapping_path,
    )
    tb_scores = geo.harmonize_countries(
        df=tb_scores,
        countries_file=paths.country_mapping_path,
    )

    # Create indicator for electoral democracy
    tb_scores = add_electdem(tb_scores)

    # Merge
    tb = tb_ratings.merge(tb_scores, on=["country", "year"], how="outer")

    # Drop rows without values
    columns_excluded = ["country", "year", "country_fh"]
    tb = tb.dropna(subset=[col for col in tb.columns if col not in columns_excluded], how="all")
    tb = cast(Table, tb)

    # Impute values
    col_flag_imputed = "values_imputed"
    assert (
        tb["year"].min() == YEAR_MIN
    ), f"Minimum year is not as expected (should be {YEAR_MIN}! Imputing might behave unexpectedly."
    tb = add_imputes(tb=tb, path=PATH_IMPUTE, col_flag_imputed=col_flag_imputed)

    # Get region data
    tb_regions = tb.loc[~tb[col_flag_imputed]].drop(columns=[col_flag_imputed]).copy()
    tb_regions = get_region_aggregates(tb_regions, ds_regions, ds_population)

    # Clear variables before they are included
    # tb_regions.loc[tb_regions["year"] < 2005, "num_regime_fh"] = pd.NA
    tb_regions.loc[tb_regions["year"] < 2005, "num_electdem_fh"] = pd.NA
    tb_regions.loc[tb_regions["year"] < 2005, "pop_electdem_fh"] = pd.NA
    tb_regions.loc[(tb_regions["year"] == 1981) | (tb_regions["year"] < 1972), "pop_regime_fh"] = pd.NA
    # tb_regions.loc[(tb_regions["year"] == 1981) & (tb_regions["year"] < 1972), "pop_electdem_fh"] = pd.NA

    # Remove imputed flag
    tb = tb.drop(columns=[col_flag_imputed])

    # Table list
    tables = [
        tb.format(["country", "year"], short_name=paths.short_name),
        tb_regions.format(["country", "year", "category"], short_name=paths.short_name + "_regions"),
    ]

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=tables,
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_electdem(tb: Table) -> Table:
    """Add electoral democracy indicator."""
    mask = (tb["electprocess_fh"] >= 7) & (tb["polrights_score_fh"] >= 20) & (tb["civlibs_score_fh"] >= 30)
    tb.loc[mask, "electdem_fh"] = 1
    tb.loc[
        ~mask & (tb[["electprocess_fh", "polrights_score_fh", "civlibs_score_fh"]].notna().all(axis=1)),
        "electdem_fh",
    ] = 0

    tb["electdem_fh"] = tb["electdem_fh"].copy_metadata(tb["electprocess_fh"])
    return tb


def get_region_aggregates(
    tb: Table,
    ds_regions: Dataset,
    ds_population: Dataset,
) -> Table:
    """Create table with region aggregates.

    Includes counts of countries and counts of people living in countries"""
    tb_ = tb.copy()

    # Set INTs
    tb_ = tb_.astype(
        {
            "regime_fh": "Int64",
            "electdem_fh": "Int64",
        }
    )
    tb_ = cast(Table, tb_)

    # Define columns on which we will estimate (i) "number of countries" and (ii) "number of people living in ..."
    indicators = [
        {
            "name": "regime_fh",
            "values_expected": {
                "0": "Not free",
                "1": "Partly free",
                "2": "Free",
            },
            "has_na": True,
        },
        {
            "name": "electdem_fh",
            "values_expected": {
                "0": "Non-electoral democracy",
                "1": "Electoral democracy",
            },
            "has_na": True,
        },
    ]
    indicator_names = [indicator["name"] for indicator in indicators]

    # 1) numbers
    ## Make dummies
    tb_num = make_table_with_dummies(tb_, indicators)

    ## Count
    tb_num = add_regions_and_global_aggregates(tb_num, ds_regions, regions=REGIONS)
    tb_num = from_wide_to_long(tb_num)
    tb_num = tb_num.rename(columns=dict(zip(indicator_names, [f"num_{i}" for i in indicator_names])))

    # 2) Get people
    ## Get missing years (not to miss anyone!) -- Note that this can lead to country overlaps (e.g. USSR and Latvia)
    tb_pop = expand_observations_without_duplicates(tb_)
    print(f"{tb_.shape} -> {tb_pop.shape}")

    ## Make dummies
    for ind in indicators:
        ind["has_na"] = True
    tb_pop = make_table_with_dummies(tb_pop, indicators)

    # Replace USSR -> current states
    tb_pop = replace_ussr(tb_pop, ds_regions)

    ## Counts
    tb_pop = add_population_in_dummies(tb_pop, ds_population)
    tb_pop = add_regions_and_global_aggregates(tb_pop, ds_regions, regions=REGIONS)
    tb_pop = from_wide_to_long(tb_pop)
    tb_pop = tb_pop.rename(columns=dict(zip(indicator_names, [f"pop_{i}" for i in indicator_names])))

    # 3) Merge
    tb_regions = tb_num.merge(tb_pop, on=["country", "year", "category"], how="outer")

    return tb_regions


def expand_observations_without_duplicates(tb: Table) -> Table:
    """Expand observations."""
    tb_exp = expand_observations(tb)
    tb_exp = tb_exp.loc[
        ~(
            # YUGOSLAVIA
            (
                # Keep but replace 1991-2002 with "Serbia and Montenegro"
                (tb_exp["country"] == "Yugoslavia") & (tb_exp["year"] > 2002)
            )
            | ((tb_exp["country"] == "Slovenia") & (tb_exp["year"] <= 1990))
            | ((tb_exp["country"] == "Croatia") & (tb_exp["year"] <= 1990))
            | ((tb_exp["country"] == "North Macedonia") & (tb_exp["year"] <= 1990))
            | ((tb_exp["country"] == "Bosnia and Herzegovina") & (tb_exp["year"] <= 1990))
            | ((tb_exp["country"] == "Kosovo") & (tb_exp["year"] <= 1992))
            # Serbia and Montenegro
            | ((tb_exp["country"] == "Serbia and Montenegro") & ((tb_exp["year"] > 2005) | (tb_exp["year"] < 2003)))
            | ((tb_exp["country"] == "Serbia") & (tb_exp["year"] <= 2005))
            | ((tb_exp["country"] == "Montenegro") & (tb_exp["year"] <= 2005))
            # YEMEN
            | ((tb_exp["country"] == "Yemen Arab Republic") & ((tb_exp["year"] > 1989) | (tb_exp["year"] < 1940)))
            | ((tb_exp["country"] == "Yemen People's Republic") & ((tb_exp["year"] > 1989) | (tb_exp["year"] < 1940)))
            | ((tb_exp["country"] == "Yemen") & ((tb_exp["year"] >= 1940) & (tb_exp["year"] <= 1989)))
            # GERMANY
            | ((tb_exp["country"] == "West Germany") & ((tb_exp["year"] > 1989) | (tb_exp["year"] < 1945)))
            | ((tb_exp["country"] == "East Germany") & ((tb_exp["year"] > 1989) | (tb_exp["year"] < 1945)))
            | ((tb_exp["country"] == "Germany") & (tb_exp["year"] >= 1945) & (tb_exp["year"] <= 1989))
            # USSR
            | ((tb_exp["country"] == "USSR") & (tb_exp["year"] > 1990))
            | ((tb_exp["country"] == "Uzbekistan") & (tb_exp["year"] <= 1990))
            | ((tb_exp["country"] == "Kazakhstan") & (tb_exp["year"] <= 1990))
            | ((tb_exp["country"] == "Turkmenistan") & (tb_exp["year"] <= 1990))
            | ((tb_exp["country"] == "Kyrgyzstan") & (tb_exp["year"] <= 1990))
            | ((tb_exp["country"] == "Tajikistan") & (tb_exp["year"] <= 1990))
            | ((tb_exp["country"] == "Russia") & (tb_exp["year"] <= 1990))
            | ((tb_exp["country"] == "Ukraine") & (tb_exp["year"] <= 1990))
            | ((tb_exp["country"] == "Belarus") & (tb_exp["year"] <= 1990))
            | ((tb_exp["country"] == "Moldova") & (tb_exp["year"] <= 1990))
            | ((tb_exp["country"] == "Latvia") & (tb_exp["year"] <= 1990))
            | ((tb_exp["country"] == "Lithuania") & (tb_exp["year"] <= 1990))
            | ((tb_exp["country"] == "Estonia") & (tb_exp["year"] <= 1990))
            | ((tb_exp["country"] == "Armenia") & (tb_exp["year"] <= 1990))
            | ((tb_exp["country"] == "Georgia") & (tb_exp["year"] <= 1990))
            | ((tb_exp["country"] == "Azerbaijan") & (tb_exp["year"] <= 1990))
            # CZECHOSLOVAKIA
            | ((tb_exp["country"] == "Czechoslovakia") & (tb_exp["year"] > 1992))
            | ((tb_exp["country"] == "Czechia") & (tb_exp["year"] <= 1992))
            | ((tb_exp["country"] == "Slovakia") & (tb_exp["year"] <= 1992))
        ),
    ]

    # Fix Serbia
    tb_exp.loc[
        (tb_exp["country"] == "Yugoslavia") & (tb_exp["year"] < 2003) & (tb_exp["year"] > 1990),
        "country",
    ] = "Serbia and Montenegro"

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
