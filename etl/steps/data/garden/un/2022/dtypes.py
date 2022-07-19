import pandas as pd
from pandas.api.types import CategoricalDtype
from etl.paths import BASE_DIR as base_path


_countries = pd.read_csv(
    base_path / "etl/steps/data/garden/un/2022/un_wpp.country_std.csv",
    index_col="Country",
)
countries = set(_countries["Our World In Data Name"].tolist())
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
    *{str(v) for v in range(100)},
    *{
        "10-14",
        "15-19",
        "20-24",
        "25-29",
        "30-34",
        "35-39",
        "40-44",
        "45-49",
        "50-54",
    },
    *{
        "0-4",
        "1-4",
        "5-9",
        "5-14",
        "15-24",
        "20-29",
        "25-64",
        "30-39",
        "40-49",
        "50-59",
        "60-69",
        "65-",
        "70-79",
        "80-89",
        "90-99",
        "100-",
        "100+",  # borrar
    },
    *{"all", "at birth", "15", "65", "80"},
    *{
        "none",
    },
}

# Type
dtypes = {
    "location": CategoricalDtype(categories=countries),
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
