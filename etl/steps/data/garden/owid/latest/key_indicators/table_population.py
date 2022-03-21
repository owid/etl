#
#  table_population.py
#  key_indicators
#

"""
Adapted from Ed's importers script:

https://github.com/owid/importers/blob/master/population/etl.py
"""

from pathlib import Path
from typing import cast, List

import pandas as pd

from owid.catalog import Dataset, Table
from etl.paths import DATA_DIR
from etl import data_helpers

UNWPP = DATA_DIR / "garden/wpp/2019/standard_projections"
GAPMINDER = DATA_DIR / "garden/gapminder/2019-12-10/population"
HYDE = DATA_DIR / "garden/hyde/2017/baseline"
WB_INCOME = DATA_DIR / "garden/wb/2021-07-01/wb_income"
REFERENCE = DATA_DIR / "reference"

DIR_PATH = Path(__file__).parent


def make_table() -> Table:
    t = (
        make_combined()
        .pipe(select_source)
        .pipe(data_helpers.calculate_region_sums)
        .pipe(add_income_groups)
        .pipe(prepare_dataset)
    )

    t.update_metadata_from_yaml(DIR_PATH / "table_population.meta.yml", "population")

    return t


def select_source(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rows are selected according to the following logic: "unwpp" > "gapminder" > "hyde"
    """
    df = df.loc[df.population > 0]

    # If a country has UN data, then remove all non-UN data after 1949
    has_un_data = set(df.loc[df.source == "unwpp", "country"])
    df = df.loc[
        -((df.country.isin(has_un_data)) & (df.year >= 1950) & (df.source != "unwpp"))
    ]

    # If a country has Gapminder data, then remove all non-Gapminder data between 1800 and 1949
    has_gapminder_data = set(df.loc[df.source == "gapminder", "country"])
    df = df.loc[
        -(
            (df.country.isin(has_gapminder_data))
            & (df.year >= 1800)
            & (df.year <= 1949)
            & (df.source != "gapminder")
        )
    ]

    # Test if all countries have only one row per year
    _assert_unique(df, subset=["country", "year"])

    return df.drop(columns=["source"])


def add_income_groups(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add population of income groups to the dataframe.
    """
    income_groups = Dataset(WB_INCOME)["wb_income_group"]

    population_income_groups = (
        df.merge(income_groups, left_on="country", right_index=True)
        .groupby(["income_group", "year"], as_index=False)
        .sum()
        .rename(columns={"income_group": "country"})
    )

    return pd.concat([df, population_income_groups], ignore_index=True)


def _assert_unique(df: pd.DataFrame, subset: List[str]) -> None:
    """Make sure dataframe have only one row per subset"""
    # NOTE: this could be moved to helpers
    df_deduped = df.drop_duplicates(subset=subset)
    if df.shape != df_deduped.shape:
        diff = df[~df.index.isin(df_deduped.index)]
        raise AssertionError(f"Duplicate rows:\n {diff}")


def prepare_dataset(df: pd.DataFrame) -> Table:
    df = cast(pd.DataFrame, df[df.population > 0].copy())
    df["population"] = df.population.astype("int64")
    df.year = df.year.astype(int)

    # Add a metric "% of world population"
    world_pop = df[df.country == "World"][["year", "population"]].rename(
        columns={"population": "world_pop"}
    )
    df = df.merge(world_pop, on="year", how="left")
    df["world_pop_share"] = (df["population"].div(df.world_pop)).round(4)

    df = df.drop(columns="world_pop").sort_values(["country", "year"])

    t = Table(df.set_index(["country", "year"]))
    t.population.title = "Total population (Gapminder, HYDE & UN)"
    t.world_pop_share.title = "Share of World Population"
    return t


def load_unwpp() -> pd.DataFrame:
    df = Dataset(UNWPP)["total_population"]
    df = df.reset_index().rename(
        columns={
            "population_total": "population",
        }
    )
    df = (
        df[df.variant == "Medium"]
        .drop(columns="variant")
        .assign(
            source="unwpp", population=lambda df: df.population.mul(1000).astype(int)
        )[["country", "year", "population", "source"]]
    )
    return cast(pd.DataFrame, df)


def make_combined() -> pd.DataFrame:
    unwpp = load_unwpp()

    gapminder = Dataset(GAPMINDER)["population"]
    gapminder["source"] = "gapminder"
    gapminder.reset_index(inplace=True)

    hyde = Dataset(HYDE)["population"]
    hyde["source"] = "hyde"
    hyde.reset_index(inplace=True)

    return pd.DataFrame(pd.concat([gapminder, hyde, unwpp], ignore_index=True))


if __name__ == "__main__":
    t = make_table()
