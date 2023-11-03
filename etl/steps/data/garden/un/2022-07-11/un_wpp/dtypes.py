import json
from pathlib import Path

from owid.catalog import Table
from pandas.api.types import CategoricalDtype

with open(Path(__file__).parent / "un_wpp.countries.json") as f:
    countries = sorted(set(json.load(f).values()))

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
            "constant fertility",
            "constant mortality",
            "estimates",
            "high",
            "instant replacement",
            "instant replacement zero migration",
            "low",
            "lower 80 pi",
            "lower 95 pi",
            "median pi",
            "medium",
            "momentum",
            "no change",
            "upper 80 pi",
            "upper 95 pi",
            "zero migration",
        ]
    ),
    "metric": CategoricalDtype(categories=metrics),
}
dtypes_simple = {
    **{k: "category" for k, _ in dtypes.items() if k != "year"},
    **{"year": "uint16"},
}


def optimize_dtypes(df: Table, simple: bool = False) -> Table:
    if simple:
        return df.astype(dtypes_simple)
    return df.astype(dtypes)
