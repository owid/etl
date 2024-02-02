"""Demographics table."""
from typing import Any, Dict

from owid.catalog import Table

from .dtypes import optimize_dtypes

# rename columns
COLUMNNS_ID = {
    "location": "location",
    "time": "year",
    "variant": "variant",
}
COLUMNS_METRICS: Dict[str, Dict[str, Any]] = {
    "popdensity": {
        "name": "population_density",
        "sex": "all",
        "age": "all",
    },
    "popgrowthrate": {
        "name": "growth_rate",
        "sex": "all",
        "age": "all",
        "operation": lambda x: (x).round(2),
    },
    "natchangert": {
        "name": "growth_natural_rate",
        "sex": "all",
        "age": "all",
        "operation": lambda x: (x / 10).round(2),
    },
    "births": {
        "name": "births",
        "sex": "all",
        "age": "all",
        "operation": lambda x: (x * 1000),
    },
    "cbr": {
        "name": "birth_rate",
        "sex": "all",
        "age": "all",
    },
    "tfr": {
        "name": "fertility_rate",
        "sex": "all",
        "age": "all",
    },
    "deaths": {
        "name": "deaths",
        "sex": "all",
        "age": "all",
        "operation": lambda x: (x * 1000).round(0),
    },
    "deathsfemale": {
        "name": "deaths",
        "sex": "female",
        "age": "all",
        "operation": lambda x: (x * 1000).round(0),
    },
    "deathsmale": {
        "name": "deaths",
        "sex": "male",
        "age": "all",
        "operation": lambda x: (x * 1000).round(0),
    },
    "cdr": {
        "name": "death_rate",
        "sex": "all",
        "age": "all",
    },
    "medianagepop": {
        "name": "median_age",
        "sex": "all",
        "age": "all",
        "operation": lambda x: (x).round(1),
    },
    "lex": {
        "name": "life_expectancy",
        "sex": "all",
        "age": "at birth",
        "operation": lambda x: (x).round(1),
    },
    "lexfemale": {
        "name": "life_expectancy",
        "sex": "female",
        "age": "at birth",
        "operation": lambda x: (x).round(1),
    },
    "lexmale": {
        "name": "life_expectancy",
        "sex": "male",
        "age": "at birth",
        "operation": lambda x: (x).round(1),
    },
    "le15": {
        "name": "life_expectancy",
        "sex": "all",
        "age": "15",
        "operation": lambda x: (15 + x).round(1),
    },
    "le15female": {
        "name": "life_expectancy",
        "sex": "female",
        "age": "15",
        "operation": lambda x: (15 + x).round(1),
    },
    "le15male": {
        "name": "life_expectancy",
        "sex": "male",
        "age": "15",
        "operation": lambda x: (15 + x).round(1),
    },
    "le65": {
        "name": "life_expectancy",
        "sex": "all",
        "age": "65",
        "operation": lambda x: (65 + x).round(1),
    },
    "le65female": {
        "name": "life_expectancy",
        "sex": "female",
        "age": "65",
        "operation": lambda x: (65 + x).round(1),
    },
    "le65male": {
        "name": "life_expectancy",
        "sex": "male",
        "age": "65",
        "operation": lambda x: (65 + x).round(1),
    },
    "le80": {
        "name": "life_expectancy",
        "sex": "all",
        "age": "80",
        "operation": lambda x: (80 + x).round(1),
    },
    "le80female": {
        "name": "life_expectancy",
        "sex": "female",
        "age": "80",
        "operation": lambda x: (80 + x).round(1),
    },
    "le80male": {
        "name": "life_expectancy",
        "sex": "male",
        "age": "80",
        "operation": lambda x: (80 + x).round(1),
    },
    "srb": {
        "name": "sex_ratio",
        "sex": "none",
        "age": "at birth",
        "operation": lambda x: (x).round(2),
    },
    "netmigrations": {
        "name": "net_migration",
        "sex": "all",
        "age": "all",
        "operation": lambda x: (x * 1000),
    },
    "cnmr": {
        "name": "net_migration_rate",
        "sex": "all",
        "age": "all",
    },
    "imr": {
        "name": "infant_mortality_rate",
        "sex": "all",
        "age": "0",
        "operation": lambda x: (x / 10),
    },
    "q5": {
        "name": "child_mortality_rate",
        "sex": "all",
        "age": "0-4",
        "operation": lambda x: (x / 10),
    },
}
COLUMNS_ORDER = ["location", "year", "metric", "sex", "age", "variant", "value"]


def process(df: Table, country_std: str) -> Table:
    # Unpivot
    df = df.reset_index()
    df = df.melt(COLUMNNS_ID.keys(), COLUMNS_METRICS.keys(), "metric", "value")
    # Add columns, rename columns
    df = df.rename(columns=COLUMNNS_ID)
    df = df.assign(
        # metric=df_4.metric.map({k: v["name"] for k, v in columns_metrics.items()}),
        sex=df.metric.map({k: v["sex"] for k, v in COLUMNS_METRICS.items()}),
        age=df.metric.map({k: v["age"] for k, v in COLUMNS_METRICS.items()}),
        variant=df.variant.apply(lambda x: x.lower()),
        location=df.location.map(country_std),
    )
    # Dtypes
    df = optimize_dtypes(df, simple=True)
    # Scale units
    ops = {k: v.get("operation", lambda x: x) for k, v in COLUMNS_METRICS.items()}
    for m in df.metric.unique():
        df.loc[df.metric == m, "value"] = ops[m](df.loc[df.metric == m, "value"])
    # Metric name
    df = df.assign(metric=df.metric.map({k: v["name"] for k, v in COLUMNS_METRICS.items()}))
    # Column order
    df = df[COLUMNS_ORDER]
    # Discard unmapped regions
    df = df.dropna(subset=["location"])
    # dtypes
    df = optimize_dtypes(df)
    return df
