"""Fertility table."""
from typing import Any, Dict, List

from owid.catalog import Table

from .dtypes import optimize_dtypes

# rename columns
COLUMNS_ID: Dict[str, str] = {
    "location": "location",
    "time": "year",
    "variant": "variant",
    "agegrp": "age",
}
COLUMNS_METRICS: Dict[str, Dict[str, Any]] = {
    "asfr": {
        "name": "fertility_rate",
        "sex": "all",
        "operation": lambda x: x,  # (x).round(2),
    },
    "births": {
        "name": "births",
        "sex": "all",
        "operation": lambda x: (x * 1000),
    },
}
COLUMNS_ORDER: List[str] = [
    "location",
    "year",
    "metric",
    "sex",
    "age",
    "variant",
    "value",
]


def process(df: Table, country_std: str) -> Table:
    # Unpivot
    df = df.reset_index()
    df = df.melt(COLUMNS_ID.keys(), COLUMNS_METRICS.keys(), "metric", "value")
    # Add columns, rename columns
    df = df.rename(columns=COLUMNS_ID)
    df = df.assign(
        metric=df.metric.map({k: v["name"] for k, v in COLUMNS_METRICS.items()}),
        sex=df.metric.map({k: v["sex"] for k, v in COLUMNS_METRICS.items()}),
        variant=df.variant.apply(lambda x: x.lower()),
        location=df.location.map(country_std),
    )
    # dtypes
    df = optimize_dtypes(df, simple=True)
    # Column order
    df = df[COLUMNS_ORDER]
    # Discard unmapped regions
    df = df.dropna(subset=["location"])
    # Scale units
    ops = {v["name"]: v.get("operation", lambda x: x) for k, v in COLUMNS_METRICS.items()}
    for m in df.metric.unique():
        df.loc[df.metric == m, "value"] = ops[m](df.loc[df.metric == m, "value"])
    df = optimize_dtypes(df)
    return df
