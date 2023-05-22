"""Generate GHE garden dataset"""

from typing import cast

import pandas as pd
from owid.catalog import Dataset, Table
from owid.datautils import dataframes
from structlog import get_logger

from etl.data_helpers.population import add_population
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# naming conventions
N = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("ghe: start")

    # read dataset from meadow
    ds_meadow = N.meadow_dataset
    df = pd.DataFrame(ds_meadow["ghe"])

    # convert codes to country names
    code_to_country = cast(Dataset, N.load_dependency("regions"))["regions"]["name"].to_dict()
    df["country"] = dataframes.map_series(df["country"], code_to_country, warn_on_missing_mappings=True)

    df = clean_data(df)

    ds_garden = create_dataset(dest_dir, tables=[Table(df, short_name="ghe")], default_metadata=ds_meadow.metadata)
    ds_garden.save()

    log.info("ghe.end")


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    log.info("ghe: basic cleaning")
    df["sex"] = df["sex"].map({"BTSX": "Both sexes", "MLE": "Male", "FMLE": "Female"})
    df[["daly_rate100k", "daly_count", "death_rate100k"]] = df[["daly_rate100k", "daly_count", "death_rate100k"]].round(
        2
    )
    df["death_count"] = df["death_count"].round(0)
    df["death_count"] = df["death_count"].astype(int)

    # Add broader age groups
    df = build_custom_age_groups(df)
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
            "death_count": "int32",
            "death_rate100k": "float32",
            "flag_level": "uint8",
        }
    )
    return df.set_index(["country", "year", "age_group", "sex", "cause"])


def build_custom_age_groups(df: pd.DataFrame) -> pd.DataFrame:
    """Estimate all the metrics for a broader age group."""
    df = pd.DataFrame(df)

    # Get two dfs: one with cause=self-harm, the other with the rest
    log.info("ghe: separate self-harm from other causes")
    msk = (df["cause"].isin(["Self-harm"])) & (
        # The following age groups are passed as they are, without assigning them to broader age groups.
        ~df["age_group"].isin(
            [
                "ALLAges",
                "YEARS85PLUS",
                "YEARS15-19",
                "YEARS20-24",
            ]
        )
    )
    df_sh = df[msk].reset_index(drop=True).copy()
    df = df[~msk].reset_index(drop=True).copy()

    # Add population values for each dimension
    log.info("ghe: add population values")
    AGE_GROUPS_RANGES = {
        "YEARS0-1": [0, 1],
        "YEARS1-4": [1, 4],
        "YEARS5-9": [5, 9],
        "YEARS10-14": [10, 14],
        # "YEARS15-19": [15, 19],
        # "YEARS20-24": [20, 24],
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
        # "YEARS85PLUS": [85, None],
    }
    df_sh = add_population(
        df=df_sh,
        country_col="country",
        year_col="year",
        sex_col="sex",
        sex_group_all="Both sexes",
        sex_group_female="Female",
        sex_group_male="Male",
        age_col="age_group",
        age_group_mapping=AGE_GROUPS_RANGES,
    )
    # Map age groups to broader age groups. Missing age groups in the list are passed as they are (no need to assign to broad group)
    log.info("ghe: create broader age groups")
    AGE_GROUPS = {
        "YEARS0-1": "YEARS0-14",
        "YEARS1-4": "YEARS0-14",
        "YEARS5-9": "YEARS0-14",
        "YEARS10-14": "YEARS0-14",
        # "YEARS15-19": "YEARS15-24",
        # "YEARS20-24": "YEARS15-24",
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
        # "YEARS85PLUS": "YEARS85PLUS",
    }
    df_sh["age_group"] = df_sh["age_group"].map(AGE_GROUPS)

    # Sum
    log.info("ghe: re-estimate metrics")
    df_sh = df_sh.groupby(["country", "year", "age_group", "sex", "cause"], as_index=False, observed=True).sum()
    # Fix column values (rates + flag)
    df_sh["daly_rate100k"] = 100000 * df_sh["daly_count"] / df_sh["population"]
    df_sh["death_rate100k"] = 100000 * df_sh["death_count"] / df_sh["population"]
    df_sh["flag_level"] = 3
    df_sh = df_sh.drop(columns=["population"])

    log.info("ghe: concatenate dfs")
    df = pd.concat([df, df_sh], axis=0, ignore_index=True, sort=False)
    return df
