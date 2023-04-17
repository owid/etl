from pathlib import Path

import pandas as pd
from pandas.api.types import CategoricalDtype

_countries = pd.read_csv(
    Path(__file__).parent / "un_wpp.country_std.csv",
    index_col="Country",
)

countries = sorted(set(_countries["Our World In Data Name"].tolist()))
metrics = {
    "birth_rate",
    "births",
    "child_mortality_rate",
    "death_rate",
    "deaths",
    "dependency_ratio_child",
    "dependency_ratio_old",
    "dependency_ratio_total",
    "fertility_rate",
    "growth_natural_rate",
    "growth_rate",
    "infant_mortality_rate",
    "life_expectancy",
    "median_age",
    "net_migration",
    "net_migration_rate",
    "population",
    "population_broad",
    "population_change",
    "population_density",
    "sex_ratio",
}
ages = {
    *{str(v) for v in range(100)},  # 1-year age groups
    *{f"{i - i%5}-{i + 4 - i%5}" for i in range(0, 100, 5)},  # 5-year age groups
    *{f"{i - i%5}-{i + 9 - i%10}" for i in range(0, 100, 10)},  # 10-year age groups
    *{  # special age groups
        "0-14",
        "0-24",
        "1-4",
        "5-14",
        "15-24",
        "15-64",
        "20-29",
        "25-64",
        "65+",
        "100+",
        "15+",
        "18+",
        "all",
        "at birth",
        "none",
    },
}

# Type
dtypes = {
    "location": CategoricalDtype(categories=countries, ordered=True),
    "year": "uint16",
    "sex": CategoricalDtype(categories=["all", "male", "female", "none"]),
    "age": CategoricalDtype(categories=ages),
    "variant": CategoricalDtype(
        categories=[
            "estimates",
            "medium",
            "high",
            "low",
            "constant fertility",
            "instant replacement",
            "zero migration",
            "constant mortality",
            "no change",
            "momentum",
            "instant replacement zero migration",
        ]
    ),
    "metric": CategoricalDtype(categories=metrics),
}
dtypes_simple = {
    **{k: "category" for k, _ in dtypes.items() if k != "year"},
    **{"year": "uint16"},
}


def optimize_dtypes(df: pd.DataFrame, simple: bool = False) -> pd.DataFrame:
    if simple:
        return df.astype(dtypes_simple)
    return df.astype(dtypes)
