#
#  table_population.py
#  key_indicators
#

"""
Adapted from Ed's importers script:

https://github.com/owid/importers/blob/master/population/etl.py
"""

from copy import deepcopy
from pathlib import Path
from typing import List, cast

import pandas as pd
from owid.catalog import Dataset, Table
from pandas.api.types import CategoricalDtype

from etl.paths import DATA_DIR
from etl.steps.data.garden.owid.latest.key_indicators.utils import add_regions

UNWPP = DATA_DIR / "garden/un/2022-07-11/un_wpp"
GAPMINDER = DATA_DIR / "garden/gapminder/2019-12-10/population"
HYDE = DATA_DIR / "garden/hyde/2017/baseline"
WB_INCOME = DATA_DIR / "garden/wb/2021-07-01/wb_income"

DIR_PATH = Path(__file__).parent


def make_table() -> Table:
    t = make_combined().pipe(select_source).pipe(add_regions).pipe(add_world).pipe(prepare_dataset)

    t.update_metadata_from_yaml(DIR_PATH / "key_indicators.meta.yml", "population")

    return t


def select_source(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rows are selected according to the following logic: "unwpp" > "gapminder" > "hyde"
    """
    df = df.loc[df.population > 0]

    # If a country has UN data, then remove all non-UN data after 1949
    has_un_data = set(df.loc[df.source == "unwpp", "country"])
    df = df.loc[-((df.country.isin(has_un_data)) & (df.year >= 1950) & (df.source != "unwpp"))]

    # If a country has Gapminder data, then remove all non-Gapminder data between 1800 and 1949
    has_gapminder_data = set(df.loc[df.source == "gapminder", "country"])
    df = df.loc[
        -((df.country.isin(has_gapminder_data)) & (df.year >= 1800) & (df.year <= 1949) & (df.source != "gapminder"))
    ]

    # Test if all countries have only one row per year
    _assert_unique(df, subset=["country", "year"])

    return df.drop(columns=["source"])


def _assert_unique(df: pd.DataFrame, subset: List[str]) -> None:
    """Make sure dataframe have only one row per subset"""
    # NOTE: this could be moved to helpers
    df_deduped = df.drop_duplicates(subset=subset)
    if df.shape != df_deduped.shape:
        diff = df[~df.index.isin(df_deduped.index)]
        raise AssertionError(f"Duplicate rows:\n {diff}")


def add_world(df: pd.DataFrame) -> pd.DataFrame:
    """Add world aggregates.

    We do this by adding the values for all continents.

    Note that for some years, there is data available for 'World'. This is because some of the datasets
    contain 'World' data but others don't.
    """
    df_ = deepcopy(df)
    year_threshold = df_[df_.country == "World"].year.min()
    assert (
        year_threshold == 1950  # This is the year that the UN data starts.
    ), "Year threshold has changed! Check if HYDE or Gapminder data now contain 'World' data."
    continents = [
        "Europe",
        "Asia",
        "North America",
        "South America",
        "Africa",
        "Oceania",
    ]
    # Estimate "World" population for years before `year_threshold` and add to original data.
    df_ = (
        df_[(df_["country"].isin(continents)) & (df_.year < year_threshold)]
        .groupby("year", as_index=False)["population"]
        .sum(numeric_only=True)
        .assign(country="World")
    )
    df = pd.concat([df, df_], ignore_index=True).sort_values(["country", "year"])
    return df


def prepare_dataset(df: pd.DataFrame) -> Table:
    df = cast(pd.DataFrame, df[df.population > 0].copy())
    df["population"] = df.population.astype("int64")
    df.year = df.year.astype(int)

    # Add a metric "% of world population"
    world_pop = df.loc[df.country == "World", ["year", "population"]].rename(columns={"population": "world_pop"})
    df = df.merge(world_pop, on="year", how="left")
    df["world_pop_share"] = (df["population"].div(df.world_pop)).round(4)

    df = df.drop(columns="world_pop").sort_values(["country", "year"])

    t = Table(df.set_index(["country", "year"]))
    t.population.title = "Total population (Gapminder, HYDE & UN)"
    t.world_pop_share.title = "Share of World Population"
    return t


def load_unwpp() -> pd.DataFrame:
    # Load
    df = Dataset(UNWPP)["population"]

    # Filter
    df = df.reset_index()
    df = df[
        (df.metric == "population") & (df.age == "all") & (df.sex == "all") & (df.variant.isin(["estimates", "medium"]))
    ]
    # Year check
    assert df[df.variant == "medium"].year.min() == 2022
    assert df[df.variant == "estimates"].year.max() == 2021

    # Rename columns, sort rows, reset index
    countries = sorted(df.location.unique())
    columns_rename = {
        "location": "country",
        "year": "year",
        "value": "population",
    }
    df = (
        df.rename(columns=columns_rename)[columns_rename.values()]
        .assign(source="unwpp")
        .astype({"source": "category", "country": CategoricalDtype(countries, ordered=True), "population": "uint64"})
        .sort_values(["country", "year"])
        .reset_index(drop=True)
    )

    # Remove special regions
    df = df[
        ~df.country.isin(
            [
                "Northern America",
                "Latin America & Caribbean",
                "Land-locked developing countries (LLDC)",
                "Latin America and the Caribbean",
                "Least developed countries",
                "Less developed regions",
                "Less developed regions, excluding China",
                "Less developed regions, excluding least developed countries",
                "More developed regions",
                "Small island developing states (SIDS)",
                "High-income countries",
                "Low-income countries",
                "Lower-middle-income countries",
                "Upper-middle-income countries",
            ]
        )
    ]

    # Check no (country, year) duplicates
    assert df.groupby(["country", "year"]).population.count().max() == 1
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
