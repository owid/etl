"""Load a meadow dataset and create a garden dataset."""

import numpy as np
from owid.catalog import Table
from owid.catalog import processing as pr

from etl.data_helpers import geo
from etl.data_helpers.population import add_population
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Regions to create aggregates for.
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

AGE_GROUPS_RANGES = {
    "0-14": [0, 14],
    "0-4": [0, 4],
    "15-24": [15, 24],
    "15plus": [15, None],
    "18plus": [18, None],
    "25-34": [25, 34],
    "35-44": [35, 44],
    "45-54": [45, 54],
    "5-14": [5, 14],
    "55-64": [55, 64],
    "65plus": [65, None],
    "all": [0, None],
}


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("burden_disaggregated")

    # Read table from meadow dataset.
    tb = ds_meadow["burden_disaggregated"].reset_index()
    # Load regions dataset.
    ds_regions = paths.load_dataset("regions")
    # Load income groups dataset.
    ds_income_groups = paths.load_dataset("income_groups")
    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb = tb.drop(columns=["measure", "unit"])
    tb = add_population_column(tb)
    tb = combining_sexes_for_all_age_groups(tb)
    tb = geo.add_regions_to_table(
        tb,
        regions=REGIONS_TO_ADD,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        aggregations={"best": "sum", "lo": "sum", "hi": "sum", "population": "sum"},
        index_columns=["country", "year", "age_group", "sex", "risk_factor"],
    )
    tb, tb_rate = calculate_incidence_rates(tb)
    tb = tb.set_index(["country", "year", "age_group", "sex", "risk_factor"], verify_integrity=True)
    tb_rate = tb_rate.set_index(["country", "year", "age_group", "sex", "risk_factor"], verify_integrity=True)
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(
        tables=[tb, tb_rate], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )
    # Save changes in the new garden dataset.
    ds_garden.save()


def combining_sexes_for_all_age_groups(tb: Table) -> Table:
    """
    Not all of the age-groups provided by the WHO have a combined value for both sexes, so we need to combine values for males and females to calculate these.
    """

    tb["age_group"] = tb["age_group"].astype("str")
    age_groups_with_both_sexes = tb[tb["sex"] == "a"]["age_group"].drop_duplicates().to_list()
    msk = tb["age_group"].isin(age_groups_with_both_sexes)
    tb_age = tb[~msk]
    tb_gr = (
        tb_age.groupby(["country", "year", "age_group", "risk_factor"], dropna=False, observed=False)[
            ["best", "lo", "hi", "population"]
        ]
        .sum()
        .reset_index()
    )
    tb_gr["sex"] = "a"
    # Set population to nan for rows where the risk factor is not "all"
    tb_gr.loc[tb_gr["risk_factor"] != "all", "population"] = np.nan
    tb = pr.concat([tb, tb_gr], axis=0, ignore_index=True, short_name=paths.short_name)

    return tb


def add_population_column(tb: Table) -> Table:
    """
    Adding the population for each age-group, in rows where the risk factor is "all".
    """
    tb_pop = tb[tb["risk_factor"] == "all"].reset_index(drop=True)
    tb_no_pop = tb[tb["risk_factor"] != "all"].reset_index(drop=True)
    tb_pop = add_population(
        df=tb_pop,
        country_col="country",
        year_col="year",
        sex_col="sex",
        sex_group_all="a",
        sex_group_female="f",
        sex_group_male="m",
        age_col="age_group",
        age_group_mapping=AGE_GROUPS_RANGES,
    )
    tb_no_pop["population"] = np.nan
    tb = pr.concat([tb_pop, tb_no_pop], axis=0, ignore_index=True, short_name=paths.short_name)
    return tb


def calculate_incidence_rates(tb: Table) -> Table:
    """
    Calculating the incidence rate per 100,000 people for each age-group.
    Return two tables, one with absolute values and one with rates, so that it is simpler to reduce the number of variables in grapher that are just NA
    """
    tb_pop = tb[tb["risk_factor"] == "all"]
    tb_no_pop = tb[tb["risk_factor"] != "all"]
    assert tb_pop["population"].isna().sum() == 0, "There are missing population values."
    tb_pop["best_rate"] = (tb_pop["best"].div(tb_pop["population"]).replace(np.inf, np.nan)) * 100000
    tb_pop["low_rate"] = (tb_pop["lo"].div(tb_pop["population"]).replace(np.inf, np.nan)) * 100000
    tb_pop["high_rate"] = (tb_pop["hi"].div(tb_pop["population"]).replace(np.inf, np.nan)) * 100000

    tb = pr.concat([tb_pop, tb_no_pop], axis=0, ignore_index=True, short_name=paths.short_name)

    # Pulling out the absolute values and the rates into two separate tables and dropping the rows where the rates are NA
    tb_abs = tb[["country", "year", "sex", "age_group", "risk_factor", "best", "lo", "hi"]].dropna(subset=["best"])
    tb_rate = tb[["country", "year", "sex", "age_group", "risk_factor", "best_rate", "low_rate", "high_rate"]].dropna(
        subset=["best_rate"]
    )
    tb_rate.metadata.short_name = paths.short_name + "_rate"
    return tb_abs, tb_rate
