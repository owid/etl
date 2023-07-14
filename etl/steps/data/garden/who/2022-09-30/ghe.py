"""Generate GHE garden dataset"""

from typing import Any, Dict, cast

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table
from owid.datautils import dataframes
from structlog import get_logger

from etl.data_helpers import geo
from etl.data_helpers.population import add_population
from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

log = get_logger()

# naming conventions
paths = PathFinder(__file__)
AGE_GROUPS_RANGES = {
    "ALLAges": [0, None],
    "YEARS0-1": [0, 1],
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


def run(dest_dir: str) -> None:
    log.info("ghe.start")

    # read dataset from meadow
    ds_meadow = paths.meadow_dataset
    df = pd.DataFrame(ds_meadow["ghe"])
    df = df.drop(columns="flag_level")
    # Load countries regions
    regions_dataset: Dataset = paths.load_dependency("regions")
    regions = regions_dataset["regions"]
    # Load WHO Standard population
    snap: Snapshot = paths.load_dependency("standard_age_distribution.csv")
    who_standard = pd.read_csv(snap.path)
    who_standard = format_who_standard(who_standard)
    # convert codes to country names
    code_to_country = cast(Dataset, paths.load_dependency("regions"))["regions"]["name"].to_dict()
    df["country"] = dataframes.map_series(df["country"], code_to_country, warn_on_missing_mappings=True)

    df = clean_data(df, regions, who_standard)

    ds_garden = create_dataset(
        dest_dir, tables=[Table(df, short_name=paths.short_name)], default_metadata=ds_meadow.metadata
    )
    ds_garden.save()

    log.info("ghe.end")


def format_who_standard(who_standard: pd.DataFrame) -> Dict[Any, Any]:
    """
    Convert who standard age distribution into a dict and combine the over 85 age-groups
    """
    under_85 = who_standard[who_standard["age_min"] < 85]
    over_85 = who_standard[who_standard["age_min"] >= 85]
    over_85["age_group"] = "YEARS85PLUS"
    over_85["age_weight"] = over_85["age_weight"].sum()
    over_85 = over_85[["age_group", "age_weight"]].drop_duplicates().reset_index(drop=True)

    under_85["age_group"] = (
        "YEARS" + under_85["age_min"].astype(str) + "-" + under_85["age_max"].astype(int).astype(str)
    )
    under_85 = under_85[["age_group", "age_weight"]].reset_index(drop=True)

    who_standard = pd.concat([under_85, over_85])
    who_standard_dict = who_standard.set_index("age_group")["age_weight"].to_dict()
    return who_standard_dict


def clean_data(df: pd.DataFrame, regions: Table, who_standard: Dict[str, float]) -> pd.DataFrame:
    log.info("ghe.basic cleaning")
    df["sex"] = df["sex"].map({"BTSX": "Both sexes", "MLE": "Male", "FMLE": "Female"})
    df = add_population(
        df=df,
        country_col="country",
        year_col="year",
        sex_col="sex",
        sex_group_all="Both sexes",
        sex_group_female="Female",
        sex_group_male="Male",
        age_col="age_group",
        age_group_mapping=AGE_GROUPS_RANGES,
    )
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
    return df.set_index(["country", "year", "age_group", "sex", "cause"], verify_integrity=True)


def add_age_standardized_metric(df: pd.DataFrame, who_standard: Dict[str, float]) -> pd.DataFrame:
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
        "YEARS55-59": "YEARS54-59",
        "YEARS60-64": "YEARS60-64",
        "YEARS65-69": "YEARS65-69",
        "YEARS70-74": "YEARS70-74",
        "YEARS75-79": "YEARS75-79",
        "YEARS80-84": "YEARS80-84",
        "YEARS85PLUS": "YEARS85PLUS",
    }

    who_df = build_custom_age_groups(df, age_groups=age_groups_who_standard)
    df_as = who_df[["country", "year", "cause", "age_group", "sex", "death_rate100k"]]
    df_as = df_as[df_as["sex"] == "Both sexes"]
    df_as["multiplier"] = df_as["age_group"].map(who_standard, na_action="ignore")
    df_as["death_rate100k"] = df_as["death_rate100k"] * df_as["multiplier"]
    df_as["age_group"] = "Age-standardized"
    df_as = (
        df_as.groupby(["country", "year", "cause", "age_group", "sex"]).sum().drop(columns="multiplier").reset_index()
    )
    df = pd.concat([df, df_as])
    return df


def add_age_groups(df: pd.DataFrame) -> pd.DataFrame:
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
    df_combined = pd.concat([df, df_age_group_ihme, df_age_group_self_harm], ignore_index=True)

    return df_combined


def combine_drug_and_alcohol(df: pd.DataFrame) -> pd.DataFrame:
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
    df = pd.concat([df, substance_agg], ignore_index=True).drop(columns=["index"])
    return df


def calculate_rates(df: pd.DataFrame) -> pd.DataFrame:
    df["daly_rate100k"] = 100000 * (df["daly_count"] / df["population"])
    df["death_rate100k"] = 100000 * (df["death_count"] / df["population"])
    df["daly_rate100k"] = np.where(df["daly_count"] == 0, 0, df["daly_rate100k"])
    df["death_rate100k"] = np.where(df["death_count"] == 0, 0, df["death_rate100k"])

    return df


def build_custom_age_groups(df: pd.DataFrame, age_groups: dict, select_causes: Any = None) -> pd.DataFrame:
    """
    Estimate metrics for broader age groups. In line with the age-groups we define in the age_groups dict:
    """
    # Add population values for each dimension
    df_age = df.copy()
    age_groups_to_drop = set(df_age["age_group"]) - set(age_groups)
    log.info(f"Not including... {age_groups_to_drop} in custom age groups")
    df_age = df_age[df_age["age_group"].isin(age_groups.keys())]
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
    comparison = total_deaths == total_deaths_check

    assert all(comparison)
    return df_age


def remove_granular_age_groups(df: pd.DataFrame, age_groups_to_keep: list[str]) -> pd.DataFrame:
    """
    Remove the small five-year age-groups that are in the original dataset
    """
    df = df[df["age_group"].isin(age_groups_to_keep)]
    assert len(df["age_group"].drop_duplicates()) == len(age_groups_to_keep)
    return df


def add_global_total(df: pd.DataFrame) -> pd.DataFrame:
    df_glob = (
        df.groupby(["year", "age_group", "sex", "cause"], observed=True)
        .agg({"daly_count": "sum", "death_count": "sum", "population": "sum"})
        .reset_index()
    )
    df_glob["country"] = "World"
    df = pd.concat([df, df_glob])

    return df


def add_regional_and_global_aggregates(df: pd.DataFrame, regions: Table) -> pd.DataFrame:
    continents = regions[regions["region_type"] == "continent"]["name"].to_list()
    cont_out = pd.DataFrame()
    for continent in continents:
        cont_df = pd.DataFrame({"continent": continent, "country": pd.Series(geo.list_countries_in_region(continent))})
        cont_out = pd.concat([cont_out, cont_df])
    df_cont = pd.merge(df, cont_out, on="country")

    assert df_cont["continent"].isna().sum() == 0

    df_cont = (
        df_cont.groupby(["year", "continent", "age_group", "sex", "cause"], observed=True)
        .agg({"daly_count": "sum", "death_count": "sum", "population": "sum"})
        .reset_index()
    )
    df_cont = df_cont.rename(columns={"continent": "country"})

    df_regions = add_global_total(df_cont)
    df_regions = calculate_rates(df_regions)

    df = pd.concat([df, df_regions])

    return df
