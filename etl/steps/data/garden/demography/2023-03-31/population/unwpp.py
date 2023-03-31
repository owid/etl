import json
from pathlib import Path
from typing import List, cast

import pandas as pd
from pandas.api.types import CategoricalDtype

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

EXCLUDE_COUNTRIES_UNWPP = paths.directory / "excluded_countries.unwpp.json"
SOURCE_NAME = "unwpp"


def load_unwpp() -> pd.DataFrame:
    # load wpp data
    ds = paths.load_dependency(short_name="un_wpp", namespace="un")
    df = ds["population"]  # type: ignore

    # Filter
    df = df.reset_index()
    df = df[
        (df["metric"] == "population")
        & (df["age"] == "all")
        & (df["sex"] == "all")
        & (df["variant"].isin(["estimates", "medium"]))
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
    assert df.loc[df["variant"] == "medium", "year"].min() == 2022
    assert df.loc[df["variant"] == "estimates", "year"].max() == 2021


def _post_sanity_checks(df: pd.DataFrame) -> None:
    assert df.groupby(["country", "year"])["population"].count().max() == 1


def load_excluded_countries(excluded_countries_path: Path) -> List[str]:
    with open(excluded_countries_path, "r") as f:
        data = json.load(f)
        assert isinstance(data, list)
    return data


def exclude_countries(df: pd.DataFrame, excluded_countries_path: Path) -> pd.DataFrame:
    excluded_countries = load_excluded_countries(excluded_countries_path)
    return cast(pd.DataFrame, df.loc[~df.country.isin(excluded_countries)])
