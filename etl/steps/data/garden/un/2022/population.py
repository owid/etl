"""Population table."""
import pandas as pd
from typing import Dict, Tuple, List, Any


# rename columns
COLUMNS_ID: Dict[str, str] = {
    "location": "location",
    "time": "year",
    "variant": "variant",
    "agegrp": "age",
}
COLUMNS_METRICS: Dict[str, Dict[str, Any]] = {
    "sex_ratio": {
        "name": "sex_ratio",
        "sex": "none",
    },
    "popmale": {
        "name": "population",
        "sex": "male",
        "operation": lambda x: (x * 1000),
    },
    "popfemale": {
        "name": "population",
        "sex": "female",
        "operation": lambda x: (x * 1000),
    },
    "poptotal": {
        "name": "population",
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


def process(df: pd.DataFrame, country_std: str) -> pd.DataFrame:
    df = pd.DataFrame(df)
    df = df.reset_index()
    df = df.assign(Location=df.location.map(country_std))
    df = df.dropna(subset=["location"])
    # Estimate sex_ratio
    df = df.assign(sex_ratio=(100 * df.popmale / df.popfemale).round(2))
    # Unpivot
    df = df.melt(COLUMNS_ID.keys(), COLUMNS_METRICS.keys(), "metric", "value")
    # Scale units
    ops = {k: v.get("operation", lambda x: x) for k, v in COLUMNS_METRICS.items()}
    for m in df.metric.unique():
        df.loc[df.metric == m, "value"] = ops[m](df.loc[df.metric == m, "value"])
    # Rename columns, metric names
    df = df.rename(columns=COLUMNS_ID)
    df = df.assign(
        metric=df.metric.map({k: v["name"] for k, v in COLUMNS_METRICS.items()}),
        sex=df.metric.map({k: v["sex"] for k, v in COLUMNS_METRICS.items()}),
        variant=df.variant.apply(lambda x: x.lower()),
    )
    # Column order
    df = df[COLUMNS_ORDER]
    # Discard unmapped regions
    df = df.dropna(subset=["location"])
    # Add metrics
    df = add_metrics(df)
    return df


def add_metrics(df: pd.DataFrame) -> pd.DataFrame:
    df_sr = _add_metric_sexratio(df)
    df_p_granular, df_p_broad = _add_metric_population(df)
    df_p_diff = _add_metric_population_change(df)
    df = pd.concat([df_sr, df_p_granular, df_p_broad, df_p_diff], ignore_index=True)
    return df


def _add_metric_sexratio(df: pd.DataFrame) -> pd.DataFrame:
    df_sr: pd.DataFrame = df[df.metric == "sex_ratio"]
    df_sr = df_sr[
        df_sr.age.isin(
            [
                "0",
                "15",
                "20",
                "30",
                "40",
                "50",
                "60",
                "70",
                "80",
                "90",
                "100+",
                "at birth",
            ]
        )
    ]
    return df_sr


def _add_metric_population(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    df_p = df[df.metric == "population"]
    # Basic age groups
    age_map = {
        **{str(i): f"{i - i%5}-{i + 4 - i%5}" for i in range(0, 20)},
        **{str(i): f"{i - i%10}-{i + 9 - i%10}" for i in range(20, 100)},
        **{"100+": "100-"},
    }
    df_p_granular = df_p.assign(age=df_p.age.map(age_map))
    df_p_granular = df_p_granular.groupby(
        ["location", "year", "metric", "sex", "age", "variant"], as_index=False
    ).sum()
    # Additional age groups
    # <1
    df_p_0 = df_p[df_p.age == "0"].copy()
    # 1-4
    df_p_1_4 = df_p[df_p.age.isin(["1", "2", "3", "4"])].copy()
    df_p_1_4 = (
        df_p_1_4.groupby(
            ["location", "year", "metric", "sex", "variant"], as_index=False
        )
        .sum()
        .assign(age="1-4")
    )
    # all
    df_p_all = (
        df_p.groupby(["location", "year", "metric", "sex", "variant"], as_index=False)
        .value.sum()
        .assign(age="all")
    )
    # Merge all age groups
    df_p_granular = pd.concat(
        [df_p_granular, df_p_0, df_p_1_4, df_p_all], ignore_index=True
    )

    df_p_broad = df_p.assign(age=df_p.age.map(map_broad_age))
    df_p_broad = df_p_broad.groupby(
        ["location", "year", "metric", "sex", "age", "variant"], as_index=False
    ).sum()
    df_p_broad = df_p_broad.assign(metric="population_broad")
    return df_p_granular, df_p_broad


def map_broad_age(age: str) -> str:
    if age == "100+":
        return "65-"
    elif int(age) < 5:
        return "0-4"
    elif int(age) < 15:
        return "5-14"
    elif int(age) < 25:
        return "15-24"
    elif int(age) < 65:
        return "25-64"
    else:
        return "65-"


def _add_metric_population_change(df_p_granular: pd.DataFrame) -> pd.DataFrame:
    pop_diff = (
        df_p_granular.sort_values("year")
        .groupby(["location", "sex", "age", "variant"])[["value"]]
        .diff()
        .assign(metric="population_change")
    )
    df_p_diff = pd.concat(
        [
            df_p_granular[
                [col for col in df_p_granular.columns if col not in ["value", "metric"]]
            ],
            pop_diff,
        ],
        axis=1,
    ).dropna(subset="value")
    return df_p_diff
