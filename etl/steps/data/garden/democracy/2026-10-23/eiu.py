"""Load a meadow dataset and create a garden dataset."""

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog import processing as pr
from owid.catalog.tables import concat

from etl.helpers import PathFinder
from etl.steps.data.garden.democracy.shared import (
    add_population_in_dummies,
    add_regions_and_global_aggregates,
    codes_to_categories,
    expand_observations,
    from_wide_to_long,
    make_table_with_dummies,
)

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
# Year range
YEAR_MIN = 2006
YEAR_MAX = 2025
# Years with interpolated values
## Gapminder interpolates 2007 and 2009. We rather drop datapoints for these years.
YEAR_INTERPOLATED = [2007, 2009]
# Category scores (not in the 2025 report or the spreadsheet EIU provided, so they end in 2024).
COLUMNS_CATEGORIES = ["elect_freefair_eiu", "funct_gov_eiu", "pol_part_eiu", "dem_culture_eiu", "civlib_eiu"]
# Minimum number of countries in the latest edition.
NUM_COUNTRIES_MIN = 167
# Tolerance when comparing the overall score with the average of the category scores (both are rounded to 2 decimals).
TOLERANCE_AVERAGE = 0.01
# Tolerance when comparing the overall score in EIU's spreadsheet with the one in the yearly reports.
TOLERANCE_SOURCES = 0.005
# Country-years where the yearly reports' overall score differs from EIU's spreadsheet, which we use.
# Moldova 2011: rounding only (the category scores average 6.326; the 2011 report says 6.33, the spreadsheet 6.32).
# Spain 2019: the 2019 report's main table says 8.29, but its other tables, all later reports and the spreadsheet say 8.18.
SCORE_DIFFERENCES_EXPECTED = {("Moldova", 2011), ("Spain", 2019)}
# Labels of the regime classification, in order. It is stored as text, so the Swimlane chart can show it.
# NOTE: On each update, check that the codes from `add_regime_identifier` still mean the regimes labeled here.
LABELS = {
    "regime_eiu": {
        0: "Authoritarian regime",
        1: "Hybrid regime",
        2: "Flawed democracy",
        3: "Full democracy",
    },
}


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("eiu")
    ds_regions = paths.load_dataset("regions")
    ds_population = paths.load_dataset("population")

    # Read tables from meadow dataset: overall score, rank and regime type from EIU's spreadsheet, and category scores
    # from the yearly reports.
    tb = ds_meadow.read("eiu")
    tb_reports = ds_meadow.read("eiu_reports")

    #
    # Process data.
    #
    tb = paths.regions.harmonize_names(tb)
    tb_reports = paths.regions.harmonize_names(tb_reports)

    # Remove years with interpolated data (2007 and 2009 are interpolated by Gapminder)
    tb_reports = tb_reports.loc[~tb_reports["year"].isin(YEAR_INTERPOLATED)]

    # Add the category scores to the spreadsheet data. The overall score comes from the spreadsheet.
    sanity_check_sources(tb, tb_reports)
    tb = pr.merge(tb, tb_reports[["country", "year"] + COLUMNS_CATEGORIES], on=["country", "year"], how="left")

    # Fix known errors in the source (see eiu.corrections.yml).
    tb = paths.apply_corrections(tb)
    sanity_check_inputs(tb)

    # Drop rank column
    tb = tb.drop(columns=["rank_eiu"])

    # Add regime identifier, check it against EIU's own labels, and drop those.
    tb = add_regime_identifier(tb)
    sanity_check_regimes(tb)
    tb = tb.drop(columns=["regime_type_eiu"])

    ##################################################
    # AGGREGATES
    # Get country-count-related data: country-averages, number of countries, ...
    tb_num_countries, tb_avg_countries = get_country_data(tb, ds_regions)

    # Get population-related data: population-weighed averages, people living in ...
    tb_num_people, tb_avg_w_countries = get_population_data(tb, ds_regions, ds_population)
    ##################################################

    # Add regions to main table
    tb = concat([tb, tb_avg_countries, tb_avg_w_countries], ignore_index=True)

    # Store the regime as text, so the Swimlane chart can show it. Done after the aggregates, which use the numeric
    # codes. The numeric score is kept for the chart that compares the classifications of several sources.
    tb["regime_eiu_score"] = tb["regime_eiu"].astype("Int64").copy_metadata(tb["regime_eiu"])
    tb = codes_to_categories(tb, LABELS)

    #
    # Save outputs.
    #
    tables = [
        tb.format(["country", "year"]),
        tb_num_countries.format(["country", "year", "category"], short_name="num_countries"),
        tb_num_people.format(["country", "year", "category"], short_name="num_people"),
    ]

    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(
        tables=tables,
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def sanity_check_sources(tb: Table, tb_reports: Table) -> None:
    """Check that EIU's spreadsheet and the yearly reports cover the same country-years, with the same overall score."""
    tb_check = pr.merge(
        tb[["country", "year", "democracy_eiu"]],
        tb_reports[["country", "year", "democracy_eiu"]],
        on=["country", "year"],
        how="outer",
        suffixes=("", "_reports"),
    )
    tb_check = tb_check[tb_check["year"] <= tb_reports["year"].max()]
    missing = tb_check.loc[
        tb_check["democracy_eiu"].isna() | tb_check["democracy_eiu_reports"].isna(), ["country", "year"]
    ]
    assert missing.empty, f"Country-years in only one of the two sources: {missing.values.tolist()}"
    diff = (tb_check["democracy_eiu"] - tb_check["democracy_eiu_reports"]).abs() > TOLERANCE_SOURCES
    differences = set(map(tuple, tb_check.loc[diff, ["country", "year"]].values.tolist()))
    assert differences == SCORE_DIFFERENCES_EXPECTED, (
        f"Overall score differs between the spreadsheet and the yearly reports for: {sorted(differences)}"
    )


def sanity_check_inputs(tb: Table) -> None:
    """Check the combined yearly data before any processing."""
    assert not tb.duplicated(subset=["country", "year"]).any(), "Duplicate country-year rows."
    # Scores are on a 0-10 scale.
    # NOTE: Burundi's 2016 electoral process score is -0.33 in Gapminder's compilation, and the overall score (2.40) is
    # consistent with it. We keep it as published; check it against EIU's 2016 report if this indicator is ever charted.
    is_exception = (tb["country"] == "Burundi") & (tb["year"] == 2016)
    for col in ["democracy_eiu"] + COLUMNS_CATEGORIES:
        assert tb.loc[~is_exception, col].dropna().between(0, 10).all(), f"{col} outside [0, 10]."
    # Every edition covers 167 countries; the latest one must not lose any.
    tb_latest = tb[tb["year"] == tb["year"].max()]
    assert tb_latest["democracy_eiu"].notna().sum() >= NUM_COUNTRIES_MIN, (
        "Latest edition has fewer countries than expected."
    )
    # The overall score is the average of the five category scores.
    tb_cat = tb.dropna(subset=COLUMNS_CATEGORIES + ["democracy_eiu"])
    diff = (tb_cat[COLUMNS_CATEGORIES].mean(axis=1) - tb_cat["democracy_eiu"]).abs()
    bad = tb_cat.loc[diff > TOLERANCE_AVERAGE, ["country", "year"]]
    assert bad.empty, f"Overall score is not the average of the category scores for: {bad.values.tolist()}"
    # Ranks follow the overall score. EIU numbers some tied countries one after another, so a tied country's rank can be
    # anywhere within the ranks its tie spans.
    for year, tb_year in tb.groupby("year"):
        rank_min = tb_year["democracy_eiu"].rank(ascending=False, method="min")
        rank_max = tb_year["democracy_eiu"].rank(ascending=False, method="max")
        assert tb_year["rank_eiu"].between(rank_min, rank_max).all(), f"Ranks don't follow scores in {year}."


def sanity_check_regimes(tb: Table) -> None:
    """Check our regime classification against EIU's labels in the latest edition."""
    # NOTE: EIU's labels for earlier editions contradict its own scores in many country-years (mostly 2011-2016, as of
    # the spreadsheet EIU sent in November 2026), which is why we derive the regime type from the score. Check them again
    # at every update.
    labels = {0: "Authoritarian", 1: "Hybrid regime", 2: "Flawed democracy", 3: "Full democracy"}
    tb_check = tb[tb["year"] == tb["year"].max()]
    mismatch = tb_check[tb_check["regime_eiu"].astype(int).map(labels) != tb_check["regime_type_eiu"].astype(str)]
    assert mismatch.empty, f"Regime differs from EIU's label for: {mismatch[['country', 'year']].values.tolist()}"


def add_regime_identifier(tb: Table) -> Table:
    """Create regime identifier."""
    # `regime_eiu`: Categorise democracy_eiu into 4 groups
    bins = [
        -0.01,
        4,
        6,
        8,
        10,
    ]
    labels = [
        0,
        1,
        2,
        3,
    ]
    tb["regime_eiu"] = pd.cut(tb["democracy_eiu"], bins=bins, labels=labels)

    # Add metadata
    tb["regime_eiu"] = tb["regime_eiu"].copy_metadata(tb["democracy_eiu"])
    return tb


def get_country_data(tb: Table, ds_regions: Dataset) -> tuple[Table, Table]:
    """Estimate number of countries in each regime, and country-average for some indicators.

    Returns two tables:

    1) tb_num_countres: Counts countries in different regimes
        regime_eiu (counts)
            - Number of authoritarian regimes
            - Number of hybrid regimes
            - Number of flawed democracies
            - Number of full democracies

    2) tb_avg_countries: Country-average for some indicators
        - democracy_eiu (country-average)

    """
    # 1/ COUNT COUNTRIES
    # Keep only non-imputed data
    tb_num = tb.copy()

    # Set INTs
    tb_num = tb_num.astype(
        {
            "regime_eiu": "Int64",
        }
    )

    # Define columns on which we will estimate (i) "number of countries" and (ii) "number of people living in ..."
    indicators = [
        {
            "name": "regime_eiu",
            "name_new": "num_regime_eiu",
            "values_expected": {
                "0": "authoritarian regime",
                "1": "hybrid regime",
                "2": "flawed democracy",
                "3": "full democracy",
            },
            "has_na": False,
        },
    ]

    # Column per indicator-dimension
    tb_num = make_table_with_dummies(tb_num, indicators)

    # Add regions and global aggregates
    tb_num = add_regions_and_global_aggregates(tb_num, ds_regions)
    tb_num = from_wide_to_long(tb_num)

    # 2/ COUNTRY-AVERAGE INDICATORS
    tb_avg = tb.copy()
    indicators_avg = [
        "democracy_eiu",
        "pol_part_eiu",
        "dem_culture_eiu",
        "elect_freefair_eiu",
        "funct_gov_eiu",
        "civlib_eiu",
    ]

    # Keep only relevant columns
    tb_avg = tb_avg.loc[:, ["year", "country"] + indicators_avg]

    # Estimate region aggregates
    tb_avg = add_regions_and_global_aggregates(
        tb=tb_avg,
        ds_regions=ds_regions,
        aggregations={k: "mean" for k in indicators_avg},  # ty: ignore
        aggregations_world={k: np.mean for k in indicators_avg},  # ty: ignore
    )

    # Keep only certain year range
    # tb_avg = tb_avg.loc[tb_avg["year"].between(YEAR_AGG_MIN, YEAR_AGG_MAX)]

    return tb_num, tb_avg


def get_population_data(tb: Table, ds_regions: Dataset, ds_population: Dataset) -> tuple[Table, Table]:
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
            "regime_eiu": "Int64",
        }
    )

    indicators = [
        {
            "name": "regime_eiu",
            "name_new": "pop_regime_eiu",
            "values_expected": {
                "0": "authoritarian regime",
                "1": "hybrid regime",
                "2": "flawed democracy",
                "3": "full democracy",
            },
            "has_na": True,
        },
    ]

    ## Get missing years (not to miss anyone!) -- Note that this can lead to country overlaps (e.g. USSR and Latvia)
    tb_ppl = expand_observations_without_duplicates(tb_ppl, ds_regions)
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
    indicators_avg = [
        "democracy_eiu",
        "pol_part_eiu",
        "dem_culture_eiu",
        "elect_freefair_eiu",
        "funct_gov_eiu",
        "civlib_eiu",
    ]

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
        aggregations={k: "sum" for k in indicators_avg} | {"population": "sum"},  # ty: ignore
        min_num_values_per_year=1,
    )

    # Normalize by region's population
    columns_index = ["year", "country"]
    columns_indicators = [col for col in tb_avg.columns if col not in columns_index + ["population"]]
    tb_avg[columns_indicators] = tb_avg[columns_indicators].div(tb_avg["population"], axis=0)
    tb_avg = tb_avg.drop(columns="population")

    # Keep only certain year range
    # tb_avg = tb_avg.loc[tb_avg["year"].between(YEAR_AGG_MIN, YEAR_AGG_MAX)]

    # tb_avg = tb_avg.rename(
    #     columns={
    #         "democracy_eiu": "democracy_eiu_weighted",
    #     }
    # )

    # tb_avg should only contain regions, not countries
    assert tb_avg["country"].isin(set(tb["country"])).sum() == 0
    tb_avg["country"] = tb_avg["country"] + " (population-weighted)"

    return tb_ppl, tb_avg


def expand_observations_without_duplicates(tb: Table, ds_regions: Dataset) -> Table:
    # Get list of regions
    tb_regions = ds_regions.read("regions")
    countries = set(tb_regions.loc[(tb_regions["region_type"] == "country") & ~(tb_regions["is_historical"]), "name"])
    countries |= set(tb["country"])

    # Full expansion
    tb_exp = expand_observations(tb, countries)

    # Limit years
    tb_exp = tb_exp.loc[~tb_exp["year"].isin(YEAR_INTERPOLATED)]

    return tb_exp
