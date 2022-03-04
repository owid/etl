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

from owid.catalog import Dataset, Table, Source
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

    sources = [
        Source(
            name="Gapminder (v6)",
            url="https://www.gapminder.org/data/documentation/gd003/",
            date_accessed="October 8, 2021",
        ),
        Source(
            name="UN (2019)",
            url="https://population.un.org/wpp/Download/Standard/Population/",
            date_accessed="October 8, 2021",
        ),
        Source(
            name="HYDE (v3.2)",
            url="https://dataportaal.pbl.nl/downloads/HYDE/",
            date_accessed="October 8, 2021",
        ),
    ]

    # table metadata
    t.metadata.short_name = "population"
    t.metadata.title = "Population (Gapminder, HYDE & UN)"
    t.metadata.description = 'Our World in Data builds and maintains a long-run dataset on population by country, region, and for the world, based on three key sources: HYDE, Gapminder, and the UN World Population Prospects. You can find more information on these sources and how our time series is constructed on this page: <a href="https://ourworldindata.org/population-sources">What sources do we rely on for population estimates?</a>'
    t.metadata.sources = sources

    # variables metadata (variable 72 in grapher)
    t.population.metadata.title = "Population"
    t.population.metadata.description = "Population by country, available from 1800 to 2021 based on Gapminder data, HYDE, and UN Population Division (2019) estimates."
    t.population.metadata.display = {"name": "Population", "includeInTable": True}

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


def rename_entities(df: pd.DataFrame) -> pd.DataFrame:
    mapping = (
        pd.read_csv(COUNTRY_MAPPING)
        .drop_duplicates()
        .rename(
            columns={
                "Country": "country",
                "Our World In Data Name": "owid_country",
            }
        )
    )
    df = df.merge(mapping, left_on="country", right_on="country", how="left")

    missing = df[pd.isnull(df["owid_country"])]
    if len(missing) > 0:
        missing = "\n".join(missing.country.unique())
        raise Exception(f"Missing entities in mapping:\n{missing}")

    df = df.drop(columns=["country"]).rename(columns={"owid_country": "country"})

    df = df.loc[-(df.country == "DROPENTITY")]
    return df


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
