"""Generate GHE garden dataset"""

import os
from typing import Any, Dict

import numpy as np
import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Dataset, Table
from owid.datautils import dataframes
from structlog import get_logger

from etl.data_helpers import geo
from etl.data_helpers.population import add_population
from etl.helpers import PathFinder, create_dataset

log = get_logger()

SUBSET = os.environ.get("SUBSET")

# naming conventions
paths = PathFinder(__file__)
AGE_GROUPS_RANGES = {
    "ALLAges": [0, None],
    "YEARS0-1": [0, 0],
    "YEARS1-4": [1, 4],
    "YEARS5-9": [5, 9],
    "YEARS10-14": [10, 14],
    "YEARS15-19": [15, 19],
    "YEARS20-24": [20, 24],
    "YEARS25-29": [25, 29],
    "YEARS30-34": [30, 34],
    "YEARS35-39": [35, 39],
    "YEARS40-44": [40, 44],
    "YEARS45-49": [45, 49],
    "YEARS50-54": [50, 54],
    "YEARS55-59": [55, 59],
    "YEARS60-64": [60, 64],
    "YEARS65-69": [65, 69],
    "YEARS70-74": [70, 74],
    "YEARS75-79": [75, 79],
    "YEARS80-84": [80, 84],
    "YEARS85PLUS": [85, None],
}

# rename GHE age groups to names we used in the previous version
AGE_GROUPS_MAP = {
    "TOTAL": "ALLAges",
    "Y0T1": "YEARS0-1",
    "Y1T4": "YEARS1-4",
    "Y5T9": "YEARS5-9",
    "Y10T14": "YEARS10-14",
    "Y15T19": "YEARS15-19",
    "Y20T24": "YEARS20-24",
    "Y25T29": "YEARS25-29",
    "Y30T34": "YEARS30-34",
    "Y35T39": "YEARS35-39",
    "Y40T44": "YEARS40-44",
    "Y45T49": "YEARS45-49",
    "Y50T54": "YEARS50-54",
    "Y55T59": "YEARS55-59",
    "Y60T64": "YEARS60-64",
    "Y65T69": "YEARS65-69",
    "Y70T74": "YEARS70-74",
    "Y75T79": "YEARS75-79",
    "Y80T84": "YEARS80-84",
    "YGE_85": "YEARS85PLUS",
}


def run(dest_dir: str) -> None:
    # read dataset from meadow
    ds_meadow = paths.load_dataset()
    tb = ds_meadow.read_table("ghe")
    tb = tb.drop(columns="flag_level")

    tb = rename_table_for_compatibility(tb)

    if SUBSET:
        required_causes = ["Drug use disorders", "Alcohol use disorders"]
        tb = tb[tb.cause.isin(SUBSET.split(",") + required_causes)]

    # Load countries regions
    regions = paths.load_dataset("regions")["regions"]
    # Load WHO Standard population
    snap = paths.load_snapshot("standard_age_distribution.csv")
    who_standard = snap.read()
    who_standard = format_who_standard(who_standard)
    # Read population dataset
    ds_population = paths.load_dataset("un_wpp")

    # convert codes to country names
    code_to_country = regions["name"].to_dict()
    tb["country"] = dataframes.map_series(tb["country"], code_to_country, warn_on_missing_mappings=True)

    # clean data, process and create final output
    tb = clean_data(tb, regions, who_standard, ds_population)

    # format
    tb = tb.format(["country", "year", "age_group", "sex", "cause"])

    # Get male-to-female death rate ratio
    tb_ratio = get_death_rate_sex_ratio(tb)
    tb_ratio = tb_ratio.format(["country", "year"], short_name="ghe_suicides_ratio")

    # Create tables
    tables = [
        tb,
        tb_ratio,
    ]
    ds_garden = create_dataset(dest_dir, tables=tables, default_metadata=ds_meadow.metadata)
    ds_garden.save()


def rename_table_for_compatibility(tb: Table) -> Table:
    """Rename columns and labels to be compatible with the previous version of the dataset."""
    tb.age_group = tb.age_group.map(AGE_GROUPS_MAP)
    tb = tb.rename(
        columns={
            "val_dths_count_numeric": "death_count",
            "val_dths_rate100k_numeric": "death_rate100k",
        }
    )
    return tb


def format_who_standard(who_standard: Table) -> Dict[Any, Any]:
    """
    Convert who standard age distribution into a dict and combine the over 85 age-groups
    """
    under_85 = who_standard.loc[who_standard["age_min"] < 85].copy()
    over_85 = who_standard.loc[who_standard["age_min"] >= 85].copy()
    over_85["age_group"] = "YEARS85PLUS"
    over_85["age_weight"] = over_85["age_weight"].sum()
    over_85 = over_85[["age_group", "age_weight"]].drop_duplicates().reset_index(drop=True)

    under_85["age_group"] = (
        "YEARS" + under_85["age_min"].astype(str) + "-" + under_85["age_max"].astype(int).astype(str)
    )
    under_85 = under_85[["age_group", "age_weight"]].reset_index(drop=True)

    who_standard = pr.concat([under_85, over_85])
    who_standard_dict = who_standard.set_index("age_group")["age_weight"].to_dict()
    return who_standard_dict


def clean_data(df: Table, regions: Table, who_standard: Dict[str, float], ds_population: Dataset) -> Table:
    log.info("ghe.basic cleaning")
    df["sex"] = df["sex"].map({"TOTAL": "Both sexes", "MALE": "Male", "FEMALE": "Female"})
    df = Table(
        add_population(
            df=df,
            country_col="country",
            year_col="year",
            sex_col="sex",
            sex_group_all="Both sexes",
            sex_group_female="Female",
            sex_group_male="Male",
            age_col="age_group",
            age_group_mapping=AGE_GROUPS_RANGES,
            ds_un_wpp=ds_population,
        )
    ).copy_metadata(df)
    assert df["population"].isna().sum() == 0

    # Combine substance and alcohol abuse
    df = combine_drug_and_alcohol(df)
    # Add global and regional values
    df = add_regional_and_global_aggregates(df, regions)
    # Add age-standardized metric
    df = add_age_standardized_metric(df, who_standard)
    # Add broader age groups
    df = add_age_groups(df)
    # Set indices
    df = df.astype(
        {
            "country": "category",
            "year": "uint16",
            "cause": "category",
            "age_group": "category",
            "sex": "category",
            "daly_count": "float32",
            "daly_rate100k": "float32",
            "death_count": "float32",
            "death_rate100k": "float32",
        }
    )
    df = df.drop(columns=["population"])
    return df


def add_age_standardized_metric(df: Table, who_standard: Dict[str, float]) -> Table:
    """
    Using the WHO's standard population we can calculate the age-standardized metric
    Values from : https://cdn.who.int/media/docs/default-source/gho-documents/global-health-estimates/gpe_discussion_paper_series_paper31_2001_age_standardization_rates.pdf
    We multiply each death rate by five-year age-group by the average world population and then sum the values to create the age-standardized rate
    """
    age_groups_who_standard = {
        "YEARS0-1": "YEARS0-4",
        "YEARS1-4": "YEARS0-4",
        "YEARS5-9": "YEARS5-9",
        "YEARS10-14": "YEARS10-14",
        "YEARS15-19": "YEARS15-19",
        "YEARS20-24": "YEARS20-24",
        "YEARS25-29": "YEARS25-29",
        "YEARS30-34": "YEARS30-34",
        "YEARS35-39": "YEARS35-39",
        "YEARS40-44": "YEARS40-44",
        "YEARS45-49": "YEARS45-49",
        "YEARS50-54": "YEARS50-54",
        "YEARS55-59": "YEARS55-59",
        "YEARS60-64": "YEARS60-64",
        "YEARS65-69": "YEARS65-69",
        "YEARS70-74": "YEARS70-74",
        "YEARS75-79": "YEARS75-79",
        "YEARS80-84": "YEARS80-84",
        "YEARS85PLUS": "YEARS85PLUS",
    }

    who_df = build_custom_age_groups(df, age_groups=age_groups_who_standard)
    df_as = who_df[["country", "year", "cause", "age_group", "sex", "death_rate100k"]]

    # Check there are three sex groups
    assert (sex_groups := set(df_as["sex"])) == {
        "Both sexes",
        "Female",
        "Male",
    }, f"Unexpected sex groups! Review {sex_groups}"
    df_as["multiplier"] = df_as["age_group"].map(who_standard, na_action="ignore")
    assert all(df_as["multiplier"].notna())
    df_as["death_rate100k"] = df_as["death_rate100k"] * df_as["multiplier"]
    df_as["age_group"] = "Age-standardized"
    df_as = df_as.groupby(["country", "year", "cause", "age_group", "sex"]).sum()

    # check that all sum to 1
    # multiplier = df_as.query("sex == 'Both sexes'").multiplier
    # assert (multiplier > 0.99).all() and (multiplier < 1.01).all()

    # drop multiplier column
    df_as = df_as.drop(columns=["multiplier"]).reset_index()

    df = pr.concat([df, df_as])
    return df


def add_age_groups(df: Table) -> Table:
    """
    Create custom age-group aggregations, there are a range of different ones that might be useful for this dataset.
    For example, we may want to compare to other causes of death imported via the IHME dataset, so we should include age-groups that match our chosen IHME groups.

    """
    age_groups_ihme = {
        "YEARS0-1": "YEARS0-4",
        "YEARS1-4": "YEARS0-4",
        "YEARS5-9": "YEARS5-14",
        "YEARS10-14": "YEARS5-14",
        "YEARS15-19": "YEARS15-49",
        "YEARS20-24": "YEARS15-49",
        "YEARS25-29": "YEARS15-49",
        "YEARS30-34": "YEARS15-49",
        "YEARS35-39": "YEARS15-49",
        "YEARS40-44": "YEARS15-49",
        "YEARS45-49": "YEARS15-49",
        "YEARS50-54": "YEARS50-69",
        "YEARS55-59": "YEARS50-69",
        "YEARS60-64": "YEARS50-69",
        "YEARS65-69": "YEARS50-69",
        "YEARS70-74": "YEARS70+",
        "YEARS75-79": "YEARS70+",
        "YEARS80-84": "YEARS70+",
        "YEARS85PLUS": "YEARS70+",
    }

    age_groups_self_harm = {
        "YEARS0-1": "YEARS0-14",
        "YEARS1-4": "YEARS0-14",
        "YEARS5-9": "YEARS0-14",
        "YEARS10-14": "YEARS0-14",
        "YEARS15-19": "YEARS15-19",
        "YEARS20-24": "YEARS20-24",
        "YEARS25-29": "YEARS25-34",
        "YEARS30-34": "YEARS25-34",
        "YEARS35-39": "YEARS35-44",
        "YEARS40-44": "YEARS35-44",
        "YEARS45-49": "YEARS45-54",
        "YEARS50-54": "YEARS45-54",
        "YEARS55-59": "YEARS55-64",
        "YEARS60-64": "YEARS55-64",
        "YEARS65-69": "YEARS65-74",
        "YEARS70-74": "YEARS65-74",
        "YEARS75-79": "YEARS75-84",
        "YEARS80-84": "YEARS75-84",
        "YEARS85PLUS": "YEARS85PLUS",
    }

    df_age_group_ihme = build_custom_age_groups(df, age_groups=age_groups_ihme)
    df_age_group_self_harm = build_custom_age_groups(df, age_groups=age_groups_self_harm, select_causes=["Self-harm"])
    df = remove_granular_age_groups(df, age_groups_to_keep=["ALLAges", "Age-standardized"])
    df_combined = pr.concat([df, df_age_group_ihme, df_age_group_self_harm], ignore_index=True)

    return df_combined


def combine_drug_and_alcohol(df: Table) -> Table:
    substance_use_disorders = ["Drug use disorders", "Alcohol use disorders"]
    substance_df = df[df["cause"].isin(substance_use_disorders)]
    substance_agg = (
        substance_df.groupby(["country", "year", "age_group", "sex", "population"], as_index=False, observed=True)[
            ["daly_count", "death_count"]
        ]
        .sum()
        .reset_index()
    )
    substance_agg = calculate_rates(substance_agg)
    substance_agg["cause"] = "Substance use disorders"
    substance_agg = substance_agg.reset_index(drop=True)
    df = pr.concat([df, substance_agg], ignore_index=True).drop(columns=["index"])
    return df


def calculate_rates(df: Table) -> Table:
    df["daly_rate100k"] = 100000 * (df["daly_count"] / df["population"])
    df["death_rate100k"] = 100000 * (df["death_count"] / df["population"])
    df["daly_rate100k"] = np.where(df["daly_count"] == 0, 0, df["daly_rate100k"])
    df["death_rate100k"] = np.where(df["death_count"] == 0, 0, df["death_rate100k"])

    return df


def build_custom_age_groups(df: Table, age_groups: dict, select_causes: Any = None) -> Table:
    """
    Estimate metrics for broader age groups. In line with the age-groups we define in the age_groups dict:
    """
    # Add population values for each dimension
    df_age = df.copy()
    age_groups_to_drop = set(df_age["age_group"]) - set(age_groups)
    log.info(f"Not including... {age_groups_to_drop} in custom age groups")
    df_age = df_age.loc[df_age["age_group"].isin(age_groups.keys())]
    if select_causes is not None:
        log.info("Dropping unused causes...")
        msk = df_age["cause"].isin(select_causes)
        df_age = df_age[msk].reset_index(drop=True).copy()
    total_deaths = df_age.groupby(["country", "year"], observed=True, as_index=False)["death_count"].sum()
    # Map age groups to broader age groups. Missing age groups in the list are passed as they are (no need to assign to broad group)
    log.info("ghe.create_broader_age_groups")
    df_age["age_group"] = df_age["age_group"].map(age_groups, na_action="ignore")
    log.info("ghe.re-estimate metrics")

    df_age = (
        df_age.groupby(["country", "year", "age_group", "sex", "cause"], as_index=False, observed=True)
        .agg({"death_count": "sum", "daly_count": "sum", "population": "sum"})
        .reset_index()
        .drop(columns="index")
    )
    df_age = calculate_rates(df_age)

    # Checking we have the same number of deaths after aggregating age-groups
    total_deaths_check = df_age.groupby(["country", "year"], observed=True, as_index=False)["death_count"].sum()
    assert all(total_deaths == total_deaths_check)

    return df_age


def remove_granular_age_groups(df: Table, age_groups_to_keep: list[str]) -> Table:
    """
    Remove the small five-year age-groups that are in the original dataset
    """
    df = df.loc[df["age_group"].isin(age_groups_to_keep)]
    assert len(df["age_group"].drop_duplicates()) == len(age_groups_to_keep)
    return df


def add_global_total(df: Table) -> Table:
    df_glob = (
        df.groupby(["year", "age_group", "sex", "cause"], observed=True)
        .agg({"daly_count": "sum", "death_count": "sum", "population": "sum"})
        .reset_index()
    )
    df_glob["country"] = "World"
    df = pr.concat([df, df_glob])

    return df


def add_regional_and_global_aggregates(df: Table, regions: Table) -> Table:
    continents = regions[regions["region_type"] == "continent"]["name"].to_list()
    cont_out = Table()
    for continent in continents:
        cont_df = Table({"continent": continent, "country": pd.Series(geo.list_countries_in_region(continent))})
        cont_out = pr.concat([cont_out, cont_df])
    df_cont = pr.merge(df, cont_out, on="country")

    assert df_cont["continent"].isna().sum() == 0

    df_cont = (
        df_cont.groupby(["year", "continent", "age_group", "sex", "cause"], observed=True)
        .agg({"daly_count": "sum", "death_count": "sum", "population": "sum"})
        .reset_index()
    )
    df_cont = df_cont.rename(columns={"continent": "country"})

    df_regions = add_global_total(df_cont)
    df_regions = calculate_rates(df_regions)

    df = pr.concat([df, df_regions])

    return df


def get_death_rate_sex_ratio(tb: Table) -> Table:
    """Add male-to-female self-harm death rate ratio."""
    # Define copy
    tb_ = tb.reset_index().copy()

    # Define names
    causes = [
        "Self-harm",
    ]
    indicator = "death_rate100k"
    indicator_ratio = f"{indicator}_ratio"

    # Filter and get male and female tables
    mask = (tb_["age_group"] == "Age-standardized") & (tb_["cause"].isin(causes))
    columns = ["country", "year", indicator]
    tb_m = tb_.loc[(tb_["sex"] == "Male") & mask, columns]
    tb_f = tb_.loc[(tb_["sex"] == "Female") & mask, columns]

    # Merge data by year and country
    tb_ratio = tb_m.merge(tb_f, on=["country", "year"], suffixes=("_m", "_f"))
    tb_ratio[indicator_ratio] = tb_ratio[f"{indicator}_m"] / tb_ratio[f"{indicator}_f"]

    # Select relevant columns
    tb_ratio = tb_ratio[["country", "year", indicator_ratio]]

    # Remove NaNs and infinities
    tb_ratio = tb_ratio.dropna(subset=[indicator_ratio])
    tb_ratio = tb_ratio[~tb_ratio[indicator_ratio].isin([np.inf, -np.inf])]

    return tb_ratio
