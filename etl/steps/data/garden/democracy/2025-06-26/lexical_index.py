"""Load a meadow dataset and create a garden dataset."""

import ast
from typing import cast

from owid.catalog import Dataset, Table
from owid.catalog.processing import concat

from etl.helpers import PathFinder
from etl.steps.data.garden.democracy.shared import (
    add_age_groups,
    add_count_years_in_regime,
    add_imputes,
    add_population_in_dummies,
    add_regions_and_global_aggregates,
    expand_observations,
    from_wide_to_long,
    make_table_with_dummies,
)


class RegionMemberUnknownError(ValueError):
    pass


# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
PATH_IMPUTE = paths.directory / "lexical_index.countries_impute.yml"

REGIME_LABELS = {
    0: "non-electoral autocracy",
    1: "one-party autocracy",
    2: "multi-party autocracy without elected executive",
    3: "multi-party autocracy",
    4: "exclusive democracy",
    5: "male democracy",
    6: "electoral democracy",
}

REGIONS = {
    "Africa": {
        "additional_members": [
            "Cape Colony",
            "Natal",
            "Orange Free State",
            "Transvaal",
            "Somaliland",
            "Zanzibar",
        ]
    },
    "Asia": {
        "additional_members": [
            "Palestine/Gaza",
            "Palestine/West Bank",
            "Republic of Vietnam",
            "Democratic Republic of Vietnam",
            "Ottoman Empire",
            "Tibet",
        ]
    },
    "North America": {
        "additional_members": [
            "United Provinces of Central America",
            "Newfoundland",
        ]
    },
    "South America": {
        "additional_members": [
            "Great Colombia",
        ]
    },
    "Europe": {
        "additional_members": [
            "Brunswick",
            "Hamburg",
            "Hanover",
            "Mecklenburg Schwerin",
            "Hesse-Darmstadt",
            "Hesse-Kassel",
            "Nassau",
            "Oldenburg",
            "Papal States",
            "Prussia",
            "Kingdom of Sardinia",
            "Saxe-Weimar-Eisenach",
            "Kingdom of the Two Sicilies",
        ]
    },
    "Oceania": {},
}


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("lexical_index")
    ds_regions = paths.load_dataset("regions")
    ds_population = paths.load_dataset("population")

    # Read table from meadow dataset.
    tb = ds_meadow.read("lexical_index")

    #
    # Process data.
    #
    # Harmonize country names
    ## Harmonize country names
    tb = paths.regions.harmonize_names(tb)

    # Initial cleaning
    tb = preprocess(tb)

    # Create variable distinguishing between democracies and autocracies:
    tb = add_is_democracy(tb)

    # Create indicators with ages and experiences (electoral democracy and polyarchy)
    tb = add_age_and_experience(tb)

    # Create variable for universal suffrage
    tb = add_universal_suffrage(tb)

    # Add "Elections for chief executive and legislature"
    tb = add_exe_leg_elections(tb)

    # Add "Universal right to vote in practice"
    tb = add_suffrage_in_practice(tb)

    # Add "Recent electoral turnover"
    tb = add_recent_turnover(tb)

    # Dtypes
    tb = tb.astype(
        {
            "age_electdem_lied": "string",
            "age_polyarchy_lied": "string",
        }
    )

    # Checks on countries
    assert set(
        tb.loc[tb["country"].str.contains("Germany") & (tb["year"] < 1990) & (tb["year"] > 1944), "country"]
    ) == {"East Germany", "West Germany"}, "Other versions of Germany between 1944-1990!"
    assert set(
        tb.loc[tb["country"].str.contains("Germany") & ((tb["year"] >= 1990) | (tb["year"] <= 1944)), "country"]
    ) == {"Germany"}, "Other versions of Germany in ≤1944 or ≥1990!"

    # Impute values
    col_flag_imputed = "values_imputed"
    tb = add_imputes(tb=tb, path=PATH_IMPUTE, col_flag_imputed=col_flag_imputed)

    # Get region data
    tb_regions = tb.loc[~tb[col_flag_imputed]].drop(columns=[col_flag_imputed]).copy()
    tb_regions = get_region_aggregates(tb_regions, ds_regions, ds_population)

    verify_regional_aggregates(tb_regions)

    # Drop is imputed flag
    tb = tb.drop(columns=[col_flag_imputed])

    # Format
    tb = tb.format(["country", "year"])
    tb_regions = tb_regions.format(["country", "year", "category"], short_name="region_aggregates")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    tables = [
        tb,
        tb_regions,
    ]
    ds_garden = paths.create_dataset(
        tables=tables,
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def verify_regional_aggregates(tb_regions: Table) -> None:
    """Verify that the sum of regions matches the World row."""
    mask = tb_regions["country"] == "World"
    tb_s = tb_regions.loc[~mask].drop(columns="country")
    tb_s = tb_s.groupby(["year", "category"], as_index=False).sum().sort_values(["year", "category"])
    tb_w = tb_regions.loc[mask].drop(columns="country")
    tb_w = tb_w.groupby(["year", "category"], as_index=False).sum().sort_values(["year", "category"])
    keys = ["year", "category"]
    num_cols = [c for c in tb_s.columns if c.startswith("num_")]
    pop_cols = [c for c in tb_s.columns if c.startswith("pop_")]

    # num_ columns: exact match
    diff_num = compare_tables(tb_s[keys + num_cols], tb_w[keys + num_cols])
    if diff_num.any():
        print(f"WARNING: Exact mismatches (num_):\n{diff_num[diff_num > 0]}")
    else:
        print("num_ columns: regions match World totals.")

    # pop_ columns: allow 1% relative tolerance
    diff_pop = compare_tables(tb_s[keys + pop_cols], tb_w[keys + pop_cols], rtol=1e-15)
    if diff_pop.any():
        print(f"WARNING: Mismatches beyond 1% tolerance (pop_):\n{diff_pop[diff_pop > 0]}")
    else:
        print("pop_ columns: regions match World totals (within 1%).")


def compare_tables(
    tb_a: Table,
    tb_b: Table,
    keys: list[str] = ["year", "category"],
    rtol: float = 0.0,
) -> "pd.Series":
    """Compare two tables on shared keys and return mismatch counts per column.

    If rtol > 0, values are considered matching when their relative difference is within rtol.
    """
    import numpy as np
    import pandas as pd

    merged = tb_a.merge(tb_b, on=keys, suffixes=("_a", "_b"))
    value_cols = [c for c in tb_a.columns if c not in keys]

    counts = {}
    for col in value_cols:
        a, b = merged[f"{col}_a"], merged[f"{col}_b"]
        if rtol > 0:
            denom = np.maximum(np.abs(a), np.abs(b)).replace(0, np.nan)
            counts[col] = int((np.abs(a - b) / denom > rtol).sum())
        else:
            counts[col] = int((a != b).sum())

    return pd.Series(counts, dtype=int)


def preprocess(tb: Table) -> Table:
    """Pre-process data.

    Includes: removing NaNs, fixing bugs, sanity checks, renaming and selecting relevant columns.
    """
    # Rename columns of interest
    tb = rename_columns(tb)

    # HOTFIX 2 -> 1 encoding
    countries_miss_encoded = set(tb.loc[(tb["opposition_lied"] == 2) | (tb["legelec_lied"] == 2), "country"])
    assert countries_miss_encoded == {"Botswana"}
    tb.loc[tb["opposition_lied"] == 2, "opposition_lied"] = 1
    tb.loc[tb["legelec_lied"] == 2, "legelec_lied"] = 1

    # HOTFIX: if regime_lied is 7, then regime_redux_lied should be 6
    # There is an error in Argentina@2022
    tb.loc[(tb["regime_lied"] == 7), "regime_redux_lied"] = 6

    # Select relevant columns
    tb = tb.loc[
        :,
        [
            "country",
            "year",
            "regime_lied",
            "regime_redux_lied",
            "exelec_lied",
            "legelec_lied",
            "opposition_lied",
            "competition_lied",
            "male_suffrage_lied",
            "female_suffrage_lied",
            "poliberties_lied",
            # Turnover indicators
            "turnover_event",
            "turnover_period",
            "two_turnover_period",
            # Democratic transition
            "democratic_transition",
            "transition_type",
            # Democratic breakdown
            "democratic_breakdown",
            "breakdown_type",
        ],
    ]

    return tb


def rename_columns(tb: Table) -> Table:
    """Rename variables of interest."""
    tb = tb.rename(
        columns={
            "executive_elections": "exelec_lied",
            "legislative_elections": "legelec_lied",
            "multi_party_legislative_elections": "opposition_lied",
            "competitive_elections": "competition_lied",
            "political_liberties": "poliberties_lied",
            "lexical_index": "regime_redux_lied",
            "lexical_index_plus": "regime_lied",
            "male_suffrage": "male_suffrage_lied",
            "female_suffrage": "female_suffrage_lied",
        }
    )
    return tb


def add_is_democracy(tb: Table) -> Table:
    """Create columns classifying country-year into democracies:

    - `democracy_lied`: distinguishing between democracies and autocracies.
    - `is_full_democracy`: distinguishing between full democracies and the rest.
    - `is_electoral_democracy`: distinguishing between electoral democracies and the rest.

    """
    tb["democracy_lied"] = tb["regime_redux_lied"].eq(6).astype("Int64")
    tb["is_full_democracy"] = tb["regime_lied"].eq(7).astype("Int64")
    tb["is_electoral_democracy"] = tb["regime_lied"].eq(6).astype("Int64")
    return tb


def add_age_and_experience(tb: Table) -> Table:
    """Add age and experience related indicators.

    This includes:
        - Number of consecutive years in electoral democracy and polyarchy (age)
        - Number of total years in electoral democracy and polyarchy (experience)
        - Age groups for electoral democracy and polyarchy
    """
    columns = [
        ("regime_lied", "electdem_lied", 5),
        ("regime_lied", "polyarchy_lied", 6),
    ]
    # Add age and experience counts
    tb = add_count_years_in_regime(
        tb=tb,
        columns=columns,
    )

    for col in columns:
        col_age = f"age_{col[1]}"
        # Add age groups
        tb = add_age_groups(tb=tb, column=col_age, column_raw=col[0], category_names=REGIME_LABELS, threshold=col[2])

        # Replace category numbers with labels (age in *)
        mapping = {num: label for num, label in REGIME_LABELS.items() if num <= col[2]}
        mask = (tb[col_age] == 0) | (tb[col_age].isna())
        tb[col_age] = tb[col_age].astype(object)
        tb.loc[mask, col_age] = tb.loc[mask, col[0]].astype(object).replace(mapping)

    return tb


def add_universal_suffrage(tb: Table) -> Table:
    """Add general population's suffrage rights."""
    tb.loc[(tb["male_suffrage_lied"] == 0) & (tb["female_suffrage_lied"] == 0), "suffrage_lied"] = 0
    tb.loc[(tb["male_suffrage_lied"] == 1) & (tb["female_suffrage_lied"] == 0), "suffrage_lied"] = 1
    tb.loc[(tb["male_suffrage_lied"] == 0) & (tb["female_suffrage_lied"] == 1), "suffrage_lied"] = 1.5
    tb.loc[(tb["male_suffrage_lied"] == 1) & (tb["female_suffrage_lied"] == 1), "suffrage_lied"] = 2
    tb["suffrage_lied"].metadata = tb["female_suffrage_lied"].metadata

    assert (
        (tb["suffrage_lied"] == 1.5).sum() == 0
    ), "There are countries with women suffrage but not men suffrage! This is not expected and can lead to confusing visualisations."

    return tb


def add_exe_leg_elections(tb: Table) -> Table:
    """Add indicator for elections for both chief executive and legislature.

    Takes on the value of 1 if both exelec_lied and legelec_lied are 1, otherwise 0.
    """
    tb["exe_leg_elec_lied"] = ((tb["exelec_lied"] == 1) & (tb["legelec_lied"] == 1)).astype("Int64")
    tb["exe_leg_elec_lied"].metadata = tb["exelec_lied"].metadata
    return tb


def add_suffrage_in_practice(tb: Table) -> Table:
    """Add universal right to vote in practice.

    1 if universal suffrage (men + women) AND elections for both executive and legislature; 0 otherwise.
    Differs from suffrage_lied in that it also considers whether elections are actually held.
    """
    tb["suffrage_in_practice_lied"] = ((tb["suffrage_lied"] == 2) & (tb["exe_leg_elec_lied"] == 1)).astype("Int64")
    tb["suffrage_in_practice_lied"].metadata = tb["suffrage_lied"].metadata
    return tb


def add_recent_turnover(tb: Table) -> Table:
    """Add recent electoral turnover indicator.

    A country-year gets a value of 1 if a turnover_event occurred within the last 12 years
    (including the event year itself). For example, if turnover_event=1 in 2008, then
    recent_turnover_event_lied=1 from 2008 to 2019.
    """
    tb["recent_turnover_event_lied"] = 0

    for _, group in tb.groupby("country"):
        turnover_years = group.loc[group["turnover_event"] == 1, "year"].values
        if len(turnover_years) == 0:
            continue
        mask = tb["country"] == group["country"].iloc[0]
        for y in turnover_years:
            year_mask = mask & (tb["year"] >= y) & (tb["year"] < y + 12)
            tb.loc[year_mask, "recent_turnover_event_lied"] = 1

    tb["recent_turnover_event_lied"] = tb["recent_turnover_event_lied"].astype("Int64")
    tb["recent_turnover_event_lied"].metadata = tb["turnover_event"].metadata
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
            "democracy_lied": "Int64",
            "regime_lied": "Int64",
        }
    )
    tb_ = cast(Table, tb_)

    # Define columns on which we will estimate (i) "number of countries" and (ii) "number of people living in ..."
    indicators = [
        {
            "name": "democracy_lied",
            "values_expected": {"0": "autocracy", "1": "democracy"},
            "has_na": False,
        },
        {
            "name": "regime_lied",
            "values_expected": {
                "0": "non-electoral autocracy",
                "1": "one-party autocracy",
                "2": "multi-party autocracy without elected executive",
                "3": "multi-party autocracy",
                "4": "exclusive democracy",
                "5": "male democracy",
                "6": "electoral democracy",
                "7": "polyarchy",
            },
            "has_na": False,
        },
        {
            "name": "suffrage_lied",
            "values_expected": {
                "0.0": "no suffrage",
                "1.0": "male suffrage",
                "2.0": "universal suffrage",
            },
            "has_na": False,
        },
        {
            "name": "is_full_democracy",
            "values_expected": {
                "0": "no",
                "1": "yes",
            },
            "has_na": False,
        },
        {
            "name": "is_electoral_democracy",
            "values_expected": {
                "0": "no",
                "1": "yes",
            },
            "has_na": False,
        },
        {
            "name": "exe_leg_elec_lied",
            "values_expected": {
                "0": "no",
                "1": "yes",
            },
            "has_na": False,
        },
        {
            "name": "suffrage_in_practice_lied",
            "values_expected": {
                "0": "no",
                "1": "yes",
            },
            "has_na": False,
        },
        {
            "name": "turnover_event",
            "values_expected": {
                "0": "no",
                "1": "yes",
            },
            "has_na": False,
        },
        {
            "name": "turnover_period",
            "values_expected": {
                "0": "no",
                "1": "yes",
            },
            "has_na": False,
        },
        {
            "name": "recent_turnover_event_lied",
            "values_expected": {
                "0": "no",
                "1": "yes",
            },
            "has_na": False,
        },
        {
            "name": "transition_type",
            "values_expected": {
                "0": "0",
                "1": "1",
                "2": "2",
                "3": "3",
                "4": "4",
                "5": "5",
            },
            "has_na": False,
        },
        {
            "name": "breakdown_type",
            "values_expected": {
                "0": "0",
                "1": "1",
                "2": "2",
                "3": "3",
                "4": "4",
                "5": "5",
                "6": "6",
            },
            "has_na": False,
        },
    ]
    for col in ["group_age_electdem_lied", "group_age_polyarchy_lied"]:
        indicators.append(
            {
                "name": col,
                "values_expected": {v: v for v in set(tb_[col].fillna("-1"))},
                "has_na": False,
            }
        )

    indicator_names = [indicator["name"] for indicator in indicators]

    # 0) Detect unknown countries
    # Inspect the countries in the data and see if they can be mapped to regions. If not, raise error.
    region_members = paths.regions.get_regions(REGIONS.keys())
    region_members = [country for region in region_members.values() for country in region["members"]]
    countries_mappable_to_regions = set(region_members) | set(
        [country for region in REGIONS.values() for country in region.get("additional_members", [])]
    )
    countries_in_data = set(tb["country"].unique())
    countries_unmappable = countries_in_data - countries_mappable_to_regions
    if countries_unmappable:
        raise RegionMemberUnknownError(
            f"WARNING: The following countries cannot be mapped to regions. Please add them to REGIONS variable: {countries_unmappable}"
        )

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
    tb_pop = tb_pop[tb_pop["year"] >= 1800]

    # 3) Merge
    tb_regions = tb_num.merge(tb_pop, on=["country", "year", "category"], how="outer")
    # assert (tb_num.shape == tb_pop.shape) and (len(tb_num) == len(tb_regions))
    # tb_regions.loc[tb_regions["category"] == "-1", ["num_regime_ert", "num_regime_trich_ert"]] = float("nan")

    return tb_regions


def expand_observations_without_duplicates(tb: Table) -> Table:
    tb_exp = expand_observations(tb)
    tb_exp = tb_exp.loc[
        ~(
            # YUGOSLAVIA
            ((tb_exp["country"] == "Yugoslavia") & ((tb_exp["year"] > 1990) | (tb_exp["year"] < 1918)))
            | ((tb_exp["country"] == "Slovenia") & ((tb_exp["year"] >= 1918) & (tb_exp["year"] <= 1990)))
            | ((tb_exp["country"] == "North Macedonia") & ((tb_exp["year"] >= 1918) & (tb_exp["year"] <= 1990)))
            | ((tb_exp["country"] == "Croatia") & ((tb_exp["year"] >= 1918) & (tb_exp["year"] <= 1990)))
            | ((tb_exp["country"] == "Bosnia and Herzegovina") & ((tb_exp["year"] >= 1918) & (tb_exp["year"] <= 1990)))
            | ((tb_exp["country"] == "Serbia and Montenegro") & ((tb_exp["year"] > 2005) | (tb_exp["year"] <= 1990)))
            | ((tb_exp["country"] == "Serbia") & ((tb_exp["year"] > 1917) & (tb_exp["year"] <= 2005)))
            | ((tb_exp["country"] == "Montenegro") & ((tb_exp["year"] > 1914) & (tb_exp["year"] <= 2005)))
            | ((tb_exp["country"] == "Kosovo") & ((tb_exp["year"] >= 1918) & (tb_exp["year"] <= 2007)))
            # YEMEN
            | ((tb_exp["country"] == "Yemen Arab Republic") & ((tb_exp["year"] > 1989) | (tb_exp["year"] < 1940)))
            | ((tb_exp["country"] == "Yemen People's Republic") & ((tb_exp["year"] > 1989) | (tb_exp["year"] < 1940)))
            | ((tb_exp["country"] == "Yemen") & ((tb_exp["year"] >= 1940) & (tb_exp["year"] <= 1989)))
            # GERMANY
            | ((tb_exp["country"] == "West Germany") & ((tb_exp["year"] > 1989) | (tb_exp["year"] < 1945)))
            | ((tb_exp["country"] == "East Germany") & ((tb_exp["year"] > 1989) | (tb_exp["year"] < 1945)))
            | ((tb_exp["country"] == "Germany") & (tb_exp["year"] >= 1945) & (tb_exp["year"] <= 1989))
            # USSR
            | ((tb_exp["country"] == "USSR") & ((tb_exp["year"] > 1990) | (tb_exp["year"] < 1941)))
            | ((tb_exp["country"] == "Uzbekistan") & (tb_exp["year"] >= 1941) & (tb_exp["year"] <= 1990))
            | ((tb_exp["country"] == "Kazakhstan") & (tb_exp["year"] >= 1941) & (tb_exp["year"] <= 1990))
            | ((tb_exp["country"] == "Turkmenistan") & (tb_exp["year"] >= 1941) & (tb_exp["year"] <= 1990))
            | ((tb_exp["country"] == "Kyrgyzstan") & (tb_exp["year"] >= 1941) & (tb_exp["year"] <= 1990))
            | ((tb_exp["country"] == "Tajikistan") & (tb_exp["year"] >= 1941) & (tb_exp["year"] <= 1990))
            | ((tb_exp["country"] == "Russia") & (tb_exp["year"] >= 1941) & (tb_exp["year"] <= 1990))
            | ((tb_exp["country"] == "Ukraine") & (tb_exp["year"] >= 1941) & (tb_exp["year"] <= 1990))
            | ((tb_exp["country"] == "Belarus") & (tb_exp["year"] >= 1941) & (tb_exp["year"] <= 1990))
            | ((tb_exp["country"] == "Moldova") & (tb_exp["year"] >= 1941) & (tb_exp["year"] <= 1990))
            | ((tb_exp["country"] == "Latvia") & (tb_exp["year"] >= 1941) & (tb_exp["year"] <= 1990))
            | ((tb_exp["country"] == "Lithuania") & (tb_exp["year"] >= 1941) & (tb_exp["year"] <= 1990))
            | ((tb_exp["country"] == "Estonia") & (tb_exp["year"] >= 1941) & (tb_exp["year"] <= 1990))
            | ((tb_exp["country"] == "Armenia") & (tb_exp["year"] >= 1941) & (tb_exp["year"] <= 1990))
            | ((tb_exp["country"] == "Georgia") & (tb_exp["year"] >= 1941) & (tb_exp["year"] <= 1990))
            | ((tb_exp["country"] == "Azerbaijan") & (tb_exp["year"] >= 1941) & (tb_exp["year"] <= 1990))
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
