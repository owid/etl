"""Deaths table"""
from typing import Any, Dict

import pandas as pd
from owid.catalog import Table

from .dtypes import optimize_dtypes

# Initial settings
COLUMNS_ID = {
    "location": "location",
    "time": "year",
    "variant": "variant",
    "sex": "sex",
}
COLUMNS_METRICS: Dict[str, Dict[str, Any]] = {
    **{
        f"_{i}": {
            "name": "deaths",
            "age": f"{i}",
        }
        for i in range(100)
    },
    **{
        "_100plus": {
            "name": "deaths",
            "age": "100+",
        }
    },
}
MAPPING_SEX = {
    "Both": "all",
    "Female": "female",
    "Male": "male",
}
COLUMNS_ORDER = ["location", "year", "metric", "sex", "age", "variant", "value"]


def process(df: Table, country_std: str) -> Table:
    df = df.reset_index()
    # Melt
    df = df.melt(COLUMNS_ID.keys(), COLUMNS_METRICS.keys(), "metric", "value")
    # Add columns, rename columns
    df = df.rename(columns=COLUMNS_ID)
    df = df.assign(
        sex=df.sex.map(MAPPING_SEX),
        age=df.metric.map({k: v["age"] for k, v in COLUMNS_METRICS.items()}),
        variant=df.variant.apply(lambda x: x.lower()),
        location=df.location.map(country_std),
        metric="deaths",
        value=(df.value * 1000).astype(int),
    )
    df = optimize_dtypes(df, simple=True)
    # Add/Build age groups
    df = add_age_groups(df)
    # Dtypes
    df = optimize_dtypes(df)
    # Column order
    df = df[COLUMNS_ORDER]
    # Drop unmapped regions
    df = df.dropna(subset=["location"])
    return df


def add_age_groups(df: Table) -> Table:
    # <1
    df_0 = df[df.age == "0"].copy()
    # 1-4
    df_1_4 = df[df.age.isin(["1", "2", "3", "4"])].drop(columns="age").copy()
    df_1_4 = (
        df_1_4.groupby(
            ["location", "year", "metric", "sex", "variant"],
            as_index=False,
            observed=True,
        )
        .sum()
        .assign(age="1-4")
    )
    # Basic 5-year age groups
    age_map = {str(i): f"{i - i%5}-{i + 4 - i%5}" for i in range(0, 100)}
    df_5 = df.assign(age=df.age.map(age_map)).copy()
    df_5 = df_5.groupby(
        ["location", "year", "metric", "sex", "age", "variant"],
        as_index=False,
        observed=True,
    ).sum()
    # Basic 10-year age groups
    age_map = {str(i): f"{i - i%10}-{i + 9 - i%10}" for i in range(0, 100)}
    df_10 = df.assign(age=df.age.map(age_map)).copy()
    df_10 = df_10.groupby(
        ["location", "year", "metric", "sex", "age", "variant"],
        as_index=False,
        observed=True,
    ).sum()
    # 100+ age group
    df_100 = df[df.age == "100+"].copy()
    # Merge all age groups
    df = pd.concat([df_0, df_1_4, df_5, df_10, df_100], ignore_index=True)  # type: ignore
    return df
