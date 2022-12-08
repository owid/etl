import pandas as pd
from owid.catalog import Dataset
from pandas.api.types import CategoricalDtype
from utils import N, exclude_countries

from etl.paths import DATA_DIR

DATASET_UNWPP = DATA_DIR / "garden" / "un" / "2022-07-11" / "un_wpp"
EXCLUDE_COUNTRIES_UNWPP = N.directory / "excluded_countries.unwpp.json"
SOURCE_NAME = "unwpp"


def load_wpp() -> pd.DataFrame:
    # load wpp data
    ds = Dataset(DATASET_UNWPP)
    df = ds["population"]

    # Filter
    df = df.reset_index()
    df = df[
        (df.metric == "population") & (df.age == "all") & (df.sex == "all") & (df.variant.isin(["estimates", "medium"]))
    ]

    # Sanity checks
    _pre_sanity_checks(df)

    # Rename columns, sort rows, set dtypes, reset index
    countries = sorted(df.location.unique())
    columns_rename = {
        "location": "country",
        "year": "year",
        "value": "population",
    }
    df = (
        df.rename(columns=columns_rename)[columns_rename.values()]
        .assign(source=SOURCE_NAME)
        .astype({"source": "category", "country": CategoricalDtype(countries, ordered=True), "population": "uint64"})
        .sort_values(["country", "year"])
        .reset_index(drop=True)
    )

    # exclude countries/regions
    df = exclude_countries(df, EXCLUDE_COUNTRIES_UNWPP)

    # last sanity checks
    _post_sanity_checks(df)
    return df


def _pre_sanity_checks(df: pd.DataFrame) -> None:
    # check years
    assert df[df.variant == "medium"].year.min() == 2022
    assert df[df.variant == "estimates"].year.max() == 2021


def _post_sanity_checks(df: pd.DataFrame) -> None:
    assert df.groupby(["country", "year"]).population.count().max() == 1
