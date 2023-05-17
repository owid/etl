"""Population table."""
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from .dtypes import optimize_dtypes

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


def process_base(df: pd.DataFrame, country_std: str) -> pd.DataFrame:
    df = pd.DataFrame(df)
    df = df.reset_index()
    df = df.assign(location=df.location.map(country_std).astype("category"))
    # Discard unmapped regions
    df = df.dropna(subset=["location"])
    # Estimate sex_ratio
    df = df.assign(sex_ratio=(100 * df.popmale / df.popfemale).round(2))
    # Unpivot
    df = df.melt(COLUMNS_ID.keys(), COLUMNS_METRICS.keys(), "metric", "value")
    # Rename columns
    df = df.rename(columns=COLUMNS_ID)
    # dtypes
    df = df.astype({"metric": "category", "year": "uint16"})
    # Scale units
    ops = {k: v.get("operation", lambda x: x) for k, v in COLUMNS_METRICS.items()}
    for m in df.metric.unique():
        df.loc[df.metric == m, "value"] = ops[m](df.loc[df.metric == m, "value"])
    # Column value mappings
    df = df.assign(
        metric=df.metric.map({k: v["name"] for k, v in COLUMNS_METRICS.items()}).astype("category"),
        sex=df.metric.map({k: v["sex"] for k, v in COLUMNS_METRICS.items()}).astype("category"),
        variant=df.variant.apply(lambda x: x.lower()).astype("category"),
    )
    # Column order
    df = df[COLUMNS_ORDER]
    return df


def process(df: pd.DataFrame, country_std: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Re-organizes age groups and complements some metrics."""
    df_base = process_base(df, country_std)
    # Add metrics
    df = add_metrics(df_base.copy())
    # Remove potential outliers
    df = remove_outliers(df)
    return df_base, df


def add_metrics(df: pd.DataFrame) -> pd.DataFrame:
    # Build metrics (e.g. age groups)
    df_sr = _add_metric_sexratio(df)
    df_p_granular, df_p_broad = _add_metric_population(df)
    df_p_diff = _add_metric_population_change(df_p_granular)
    df_sr_all = _add_metric_sexratio_all(df_p_granular)
    # Optimize field types
    df_sr = optimize_dtypes(df_sr)
    df_sr_all = optimize_dtypes(df_sr_all)
    df_p_granular = optimize_dtypes(df_p_granular)
    df_p_broad = optimize_dtypes(df_p_broad)
    df_p_diff = optimize_dtypes(df_p_diff)
    # Concatenate
    df = pd.concat([df_sr, df_sr_all, df_p_granular, df_p_broad, df_p_diff], ignore_index=True)
    # Remove infs
    msk = np.isinf(df["value"])
    df.loc[msk, "value"] = np.nan
    return df


def remove_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """Remove outliers from the population table.

    So far, detected ones are:
        - Sex ratio for Sint Maarten
    """
    df = df.loc[
        ~(
            (df["location"] == "Sint Maarten (Dutch part)")
            & (df["metric"] == "sex_ratio")
            & (df["age"].isin(["5", "10", "30", "40"]))
        )
    ]
    return df


def _add_metric_sexratio(df: pd.DataFrame) -> pd.DataFrame:
    df_sr: pd.DataFrame = df.loc[df.metric == "sex_ratio"]
    df_sr = df_sr.loc[
        df_sr.age.isin(
            [
                "0",
                "5",
                "10",
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
    df_p = df.loc[df.metric == "population"]
    # Basic age groups
    age_map = {
        **{str(i): f"{i - i%5}-{i + 4 - i%5}" for i in range(0, 20)},
        **{str(i): f"{i - i%10}-{i + 9 - i%10}" for i in range(20, 100)},
        **{"100+": "100+"},
    }
    df_p_granular = df_p.assign(age=df_p.age.map(age_map).astype("category"))
    df_p_granular = df_p_granular.groupby(
        ["location", "year", "metric", "sex", "age", "variant"],
        as_index=False,
        observed=True,
    ).sum()
    df_p_granular = optimize_dtypes(df_p_granular, simple=True)
    # Additional age groups
    # <1
    df_p_0 = df_p[df_p.age == "0"].copy()
    df_p_0 = optimize_dtypes(df_p_0, simple=True)
    # 1
    df_p_1 = df_p[df_p.age == "1"].copy()
    df_p_1 = optimize_dtypes(df_p_1, simple=True)
    # 1-4
    df_p_1_4 = _add_age_group(df_p, 1, 4)
    # 0 - 14
    df_p_0_14 = _add_age_group(df_p, 0, 14)
    # 0 - 24
    df_p_0_24 = _add_age_group(df_p, 0, 24)
    # 15 - 64
    df_p_15_64 = _add_age_group(df_p, 15, 64)
    # 15+
    df_p_15_plus = _add_age_group(df_p, 15, 200, "15+")
    # 18+
    df_p_18_plus = _add_age_group(df_p, 18, 200, "18+")
    # all
    df_p_all = (
        df_p.groupby(
            ["location", "year", "metric", "sex", "variant"],
            as_index=False,
            observed=True,
        )
        .value.sum()
        .assign(age="all")
    )
    df_p_all = optimize_dtypes(df_p_all, simple=True)
    # Merge all age groups
    df_p_granular = pd.concat(
        [
            df_p_granular,
            df_p_0,
            df_p_0_14,
            df_p_0_24,
            df_p_1,
            df_p_1_4,
            df_p_15_64,
            df_p_15_plus,
            df_p_18_plus,
            df_p_all,
        ],
        ignore_index=True,
    ).astype({"age": "category"})
    # Broad age groups
    df_p_broad = df_p.assign(age=df_p.age.map(map_broad_age).astype("category"))
    df_p_broad = df_p_broad.groupby(
        ["location", "year", "metric", "sex", "age", "variant"],
        as_index=False,
        observed=True,
    ).sum()
    df_p_broad = df_p_broad.assign(metric="population_broad").astype({"metric": "category"})
    return df_p_granular, df_p_broad


def _add_age_group(df: pd.DataFrame, age_min: int, age_max: int, age_group: Optional[str] = None) -> pd.DataFrame:
    ages_accepted = [str(i) for i in range(age_min, age_max + 1)]
    dfx: pd.DataFrame = df.loc[df.age.isin(ages_accepted)].drop(columns="age").copy()
    dfx = dfx.groupby(
        ["location", "year", "metric", "sex", "variant"],
        as_index=False,
        observed=True,
    ).sum()
    if age_group:
        dfx = dfx.assign(age=age_group)
    else:
        dfx = dfx.assign(age=f"{age_min}-{age_max}")
    dfx = optimize_dtypes(dfx, simple=True)
    return dfx


def map_broad_age(age: str) -> str:
    if age == "100+":
        return "65+"
    elif int(age) < 5:
        return "0-4"
    elif int(age) < 15:
        return "5-14"
    elif int(age) < 25:
        return "15-24"
    elif int(age) < 65:
        return "25-64"
    else:
        return "65+"


def _add_metric_population_change(df_p_granular: pd.DataFrame) -> pd.DataFrame:
    pop_diff = (
        df_p_granular.sort_values("year")
        .groupby(["location", "sex", "age", "variant"])[["value"]]
        .diff()
        .assign(metric="population_change")
        .astype({"metric": "category"})
    )
    df_p_diff = pd.concat(
        [
            df_p_granular[[col for col in df_p_granular.columns if col not in ["value", "metric"]]],
            pop_diff,
        ],
        axis=1,
    ).dropna(subset="value")
    return df_p_diff


def _add_metric_sexratio_all(df_p_granular: pd.DataFrame) -> Any:
    # print(df_p_granular.head())
    # Check
    (df_p_granular.metric.unique() == ["population"]).all()
    # Get M/F values
    df_male = df_p_granular.loc[(df_p_granular.age == "all") & (df_p_granular.sex == "male")].rename(columns={"value": "value_male"})  # type: ignore
    df_female = df_p_granular.loc[(df_p_granular.age == "all") & (df_p_granular.sex == "female")].rename(columns={"value": "value_female"})  # type: ignore
    # Check
    assert len(df_male) == len(df_female)
    # Build df
    cols_merge = ["location", "year", "variant"]
    df_ = df_male.merge(df_female[cols_merge + ["value_female"]], on=cols_merge)
    df_ = df_.assign(
        value=(100 * df_.value_male / df_.value_female).round(2),
        metric="sex_ratio",
        age="all",
        sex="none",
    ).drop(columns=["value_male", "value_female"])
    # print(df_.head())
    return df_
