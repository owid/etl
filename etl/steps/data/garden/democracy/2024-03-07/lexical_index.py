"""Load a meadow dataset and create a garden dataset."""

import ast
from typing import cast

from owid.catalog import Dataset, Table
from owid.catalog.processing import concat
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


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("lexical_index")
    ds_regions = paths.load_dataset("regions")
    ds_population = paths.load_dataset("population")

    # Read table from meadow dataset.
    tb = ds_meadow["lexical_index"].reset_index()

    #
    # Process data.
    #
    # Initial cleaning
    tb = preprocess(tb)

    # Create variable distinguishing between democracies and autocracies:
    tb = add_is_democracy(tb)

    # Create indicators with ages and experiences (electoral democracy and polyarchy)
    tb = add_age_and_experience(tb)

    # Create variable for universal suffrage
    tb = add_universal_suffrage(tb)

    # Dtypes
    tb["age_electdem_lied"] = tb["age_electdem_lied"].astype("string")
    tb["age_polyarchy_lied"] = tb["age_polyarchy_lied"].astype("string")

    # Checks on countries
    assert set(
        tb.loc[tb["country"].str.contains("Germany") & (tb["year"] < 1990) & (tb["year"] > 1944), "country"]
    ) == {"East Germany", "West Germany"}, "Other versions of Germany!"
    assert set(
        tb.loc[tb["country"].str.contains("Germany") & ((tb["year"] >= 1990) | (tb["year"] <= 1944)), "country"]
    ) == {"Germany"}, "Other versions of Germany!"

    # Impute values
    col_flag_imputed = "values_imputed"
    tb = add_imputes(tb=tb, path=PATH_IMPUTE, col_flag_imputed=col_flag_imputed)

    # Get region data
    tb_regions = tb.loc[~tb[col_flag_imputed]].drop(columns=[col_flag_imputed]).copy()
    tb_regions = get_region_aggregates(tb_regions, ds_regions, ds_population)

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
    ds_garden = create_dataset(
        dest_dir, tables=tables, check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def preprocess(tb: Table) -> Table:
    """Pre-process data.

    Includes: removing NaNs, fixing bugs, sanity checks, renaming and selecting relevant columns.
    """
    ## Harmonize country names
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )
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
    """Create variable distinguishing between democracies and autocracies."""
    tb.loc[tb["regime_redux_lied"] == 6, "democracy_lied"] = 1
    tb.loc[(tb["regime_redux_lied"] >= 0) & (tb["regime_redux_lied"] < 6), "democracy_lied"] = 0
    tb["democracy_lied"] = tb["democracy_lied"].astype(int)
    tb["democracy_lied"].metadata = tb["regime_redux_lied"].metadata
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
        tb.loc[mask, col_age] = tb.loc[mask, col[0]].replace(mapping)

    return tb


def add_universal_suffrage(tb: Table) -> Table:
    """Add general population's suffrage rights."""
    tb.loc[(tb["male_suffrage_lied"] == 0) & (tb["female_suffrage_lied"] == 0), "suffrage_lied"] = 0
    tb.loc[(tb["male_suffrage_lied"] == 1) & (tb["female_suffrage_lied"] == 0), "suffrage_lied"] = 1
    tb.loc[(tb["male_suffrage_lied"] == 1) & (tb["female_suffrage_lied"] == 1), "suffrage_lied"] = 1.5
    tb.loc[(tb["male_suffrage_lied"] == 1) & (tb["female_suffrage_lied"] == 1), "suffrage_lied"] = 2
    tb["suffrage_lied"].metadata = tb["female_suffrage_lied"].metadata

    assert (
        (tb["suffrage_lied"] == 1.5).sum() == 0
    ), "There are countries with women suffrage but not men suffrage! This is not expected and can lead to confusing visualisations."

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
