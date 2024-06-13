"""Load a meadow dataset and create a garden dataset."""

from typing import Tuple, cast

import numpy as np
import pandas as pd
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
# Year range
YEAR_MIN = 2006
YEAR_MAX = 2023


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("eiu")
    ds_regions = paths.load_dataset("regions")
    ds_population = paths.load_dataset("population")

    # Read table from meadow dataset.
    tb = ds_meadow["eiu"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )

    # Remove years with interpolated data (2007 and 2009 are interpolated by Gapminder)
    tb = tb[~tb["year"].isin([2007, 2009])]

    # Drop rank column
    tb = tb.drop(columns=["rank_eiu"])
    tb = cast(Table, tb)

    tb = add_regime_identifier(tb)

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


def get_country_data(tb: Table, ds_regions: Dataset) -> Tuple[Table, Table]:
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
    tb_num = cast(Table, tb_num)

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
    indicators_avg = ["democracy_eiu"]

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
            "regime_eiu": "Int64",
        }
    )
    tb_ppl = cast(Table, tb_ppl)

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
    indicators_avg = ["democracy_eiu"]

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
            "democracy_eiu": "democracy_eiu_weighted",
        }
    )
    return tb_ppl, tb_avg


def expand_observations_without_duplicates(tb: Table, ds_regions: Dataset) -> Table:
    # Get list of regions
    tb_regions = ds_regions["regions"]
    countries = set(tb_regions.loc[(tb_regions["region_type"] == "country") & ~(tb_regions["is_historical"]), "name"])
    countries |= set(tb["country"])

    # Full expansion
    tb_exp = expand_observations(tb, countries)

    # Limit years
    tb_exp = tb_exp.loc[tb_exp["year"].isin(range(YEAR_MIN, YEAR_MAX + 1, 2))]

    return tb_exp
