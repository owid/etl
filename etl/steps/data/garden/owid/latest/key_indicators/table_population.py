#
#  table_population.py
#  key_indicators
#

"""
Adapted from Ed's importers script:

https://github.com/owid/importers/blob/master/population/etl.py
"""

import json
from typing import cast

import pandas as pd

from owid.catalog import Dataset, Table
from etl.paths import DATA_DIR

GAPMINDER = DATA_DIR / "garden/gapminder/2019-12-10/population"
HYDE = DATA_DIR / "garden/hyde/2017/baseline"
REFERENCE = DATA_DIR / "reference"


def make_table() -> Table:
    t = (
        make_combined()
        .pipe(select_source)
        .pipe(calculate_aggregates)
        .pipe(prepare_dataset)
    )
    t.metadata.short_name = "population"
    return t


def select_source(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rows are selected according to the following logic: "gapminder" > "hyde"
    """
    return (
        df.sort_values("source")
        .drop_duplicates(subset=["country", "year"], keep="first")
        .drop(columns=["source"])
    )


def calculate_aggregates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate our own totals according to OWID continent definitions.
    """
    df = cast(
        pd.DataFrame,
        df[
            -df.country.isin(
                [
                    "North America",
                    "South America",
                    "Europe",
                    "Africa",
                    "Asia",
                    "Oceania",
                    "World",
                ]
            )
        ],
    )

    countries = Dataset(REFERENCE)["countries_regions"]
    continent_rows = []
    for code, row in countries.iterrows():
        if pd.isnull(row.members):
            continue

        members = json.loads(row.members)
        for member in members:
            continent_rows.append(
                {"country": countries.loc[member].name, "continent": row.name}
            )

    continent_list = pd.DataFrame.from_records(continent_rows)

    continents = (
        df.merge(continent_list, on="country")
        .groupby(["continent", "year"], as_index=False)
        .sum()
        .rename(columns={"continent": "country"})
    )

    world = (
        df[["year", "population"]]
        .groupby("year")
        .sum()
        .reset_index()
        .assign(country="World")
    )

    return pd.concat([df, continents, world], ignore_index=True)


def prepare_dataset(df: pd.DataFrame) -> Table:
    df = cast(pd.DataFrame, df[df.population > 0].copy())
    df["population"] = df.population.astype("int64")

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


def make_combined() -> pd.DataFrame:
    gapminder = Dataset(GAPMINDER)["population"]
    gapminder["source"] = "gapminder"
    gapminder.reset_index(inplace=True)

    hyde = Dataset(HYDE)["population"]
    hyde["source"] = "hyde"
    hyde.reset_index(inplace=True)

    return pd.DataFrame(pd.concat([gapminder, hyde], ignore_index=True))
