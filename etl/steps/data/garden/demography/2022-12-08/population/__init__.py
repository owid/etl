"""OMM population dataset.

This dataset contains population data from various sources. It also includes former countries.

Four sources are used overall:

- After 1950:
    - UN WPP (2022)
- Before 1950:
    - Gapminder (v6): This is prioritised over HYDE.
    - HYDE (v3.2)
    - Gapminder (Systema Globalis):
        Provides data on former countries, and complements other sources with data on missing years for some countries.
        More on this dataset please refer to module gapminder_sg.
"""
import os
from copy import deepcopy
from typing import List, cast

import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore_table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder

from .gapminder import load_gapminder
from .gapminder_sg import (
    load_gapminder_sys_glob_complement,
    load_gapminder_sys_glob_former,
)
from .hyde import load_hyde
from .unwpp import load_unwpp

log = get_logger()

# naming conventions
N = PathFinder(__file__)
METADATA_PATH = os.path.join(N.directory, "meta.yml")

# sources names
# this dictionary maps source short names to complete source names
SOURCES_NAMES = {
    "unwpp": "United Nations - Population Division 2022 (https://population.un.org/wpp/Download/Standard/Population/)",
    "gapminder_v6": "Gapminder v6 (https://docs.google.com/spreadsheets/d/14_suWY8fCPEXV0MH7ZQMZ-KndzMVsSsA5HdR-7WqAC0/edit#gid=501532268)",
    "gapminder_sg": "Gapminder - Systema Globalis (https://github.com/open-numbers/ddf--gapminder--systema_globalis)",
    "hyde": "HYDE v3.2 (https://dataportaal.pbl.nl/downloads/HYDE/)",
}


def run(dest_dir: str) -> None:
    log.info("population.start")

    # create table
    tb = make_table()

    # create dataset
    log.info("population: create dataset")
    ds = Dataset.create_empty(dest_dir)

    # add table to dataset
    log.info("population: add table to dataset")
    ds.add(tb)

    # manage metadata
    log.info("population: add metadata")
    ds.update_metadata(METADATA_PATH)

    # save dataset
    ds.save()

    log.info("population.end")


def make_table() -> Table:
    tb = (
        load_data()
        .pipe(select_source)
        .pipe(add_regions)
        .pipe(add_world)
        .pipe(add_historical_regions)
        .pipe(filter_rows)
        .pipe(set_dtypes)
        .pipe(add_world_population_share)
        .pipe(df_to_table)
    )
    return tb


def load_data() -> pd.DataFrame:
    """Load data from all sources and concatenate them into a single dataframe."""
    log.info("population: loading data...")
    log.info("population: loading data (WPP)")
    unwpp = load_unwpp()
    log.info("population: loading data (Gapminder v6)")
    gapminder = load_gapminder()
    log.info("population: loading data (Gapminder - Systema Globalis)")
    gapminder_comp = load_gapminder_sys_glob_complement()
    log.info("population: loading data (Hyde)")
    hyde = load_hyde()
    tb = pd.DataFrame(pd.concat([gapminder, gapminder_comp, hyde, unwpp], ignore_index=True))
    return tb


def select_source(df: pd.DataFrame) -> pd.DataFrame:
    """Select adequate source for each country-year.

    Rows are selected based on the following relevance scale: "unwpp" > "gapminder_v6" > "hyde"
    """
    log.info("population: selecting source...")
    df = df.loc[df["population"] > 0]

    # If a country has UN data, then remove all non-UN data after 1949
    has_un_data = set(df.loc[df["source"] == "unwpp", "country"])
    df = df.loc[~((df["country"].isin(has_un_data)) & (df["year"] >= 1950) & (df["source"] != "unwpp"))]

    # If a country has Gapminder data, then remove all non-Gapminder data between 1800 and 1949
    has_gapminder_data = set(df.loc[df["source"] == "gapminder_v6", "country"])
    df = df.loc[
        ~(
            (df["country"].isin(has_gapminder_data))
            & (df["year"] >= 1800)
            & (df["year"] <= 1949)
            & (df["source"] != "gapminder_v6")
        )
    ]

    # Test if all countries have only one row per year
    _assert_unique(df, subset=["country", "year"])

    # map to source full names
    df["source"] = df["source"].map(SOURCES_NAMES)
    return df


def _assert_unique(df: pd.DataFrame, subset: List[str]) -> None:
    """Ensure that dataframe has only one row per columns in subset"""
    # NOTE: this could be moved to helpers
    df_deduped = df.drop_duplicates(subset=subset)
    if df.shape != df_deduped.shape:
        diff = df[~df.index.isin(df_deduped.index)]
        raise AssertionError(f"Duplicate rows:\n {diff}")


def add_regions(df: pd.DataFrame) -> pd.DataFrame:
    """Add continents and income groups."""
    log.info("population: adding regions...")
    regions = [
        "Europe",
        "Asia",
        "North America",
        "South America",
        "Africa",
        "Oceania",
        "High-income countries",
        "Low-income countries",
        "Lower-middle-income countries",
        "Upper-middle-income countries",
        "European Union (27)",
    ]
    # make sure to exclude regions if already present
    df = df.loc[~df["country"].isin(regions)]

    # keep sources per countries, remove from df
    # remove from df: otherwsie geo.add_region_aggregates will add this column too
    sources = df[["country", "year", "source"]].copy()
    df = df.drop(columns=["source"])

    # re-estimate region aggregates
    for region in regions:
        df = geo.add_region_aggregates(df=df, region=region, population=df)

    # add sources back
    # these are only added to countries, not aggregates
    df = df.merge(sources, on=["country", "year"], how="left")

    # add sources for region aggregates
    # this is done by taking the union of all sources for countries in the region
    for region in regions:
        members = geo.list_countries_in_region(region)
        s = df.loc[df["country"].isin(members), "source"].unique()
        sources_region = sorted(s)
        df.loc[df["country"] == region, "source"] = "; ".join(sources_region)
    return df


def add_world(df: pd.DataFrame) -> pd.DataFrame:
    """Add world aggregate.

    We do this by adding the values for all continents.

    Note that for some years, there is data available for 'World'. This is because some of the datasets
    contain 'World' data but others don't.
    """
    log.info("population: adding World...")
    df_ = deepcopy(df)
    year_threshold = df_.loc[df_["country"] == "World", "year"].min()
    assert (
        year_threshold == 1950  # This is the year that the UN data starts.
    ), "Year threshold has changed! Check if HYDE or Gapminder (or UN WPP) have data for 'World' before 1950!"
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
        df_[(df_["country"].isin(continents)) & (df_["year"] < year_threshold)]
        .groupby("year", as_index=False)["population"]
        .sum(numeric_only=True)
        .assign(country="World")
    )
    df = pd.concat([df, df_], ignore_index=True).sort_values(["country", "year"])

    # add sources for world
    df.loc[df["country"] == "World", "source"] = "; ".join(sorted(SOURCES_NAMES.values()))
    return df


def add_historical_regions(df: pd.DataFrame) -> pd.DataFrame:
    """Add historical regions.

    Systema Globalis from Gapminder contains historical regions. We add them to the data. These include
    Yugoslavia, USSR, etc.

    Note that this is added after regions and world regions have been obtained, to avoid double counting.
    """
    log.info("population: loading data (Gapminder Systema Globalis)")
    gapminder_sg = load_gapminder_sys_glob_former()
    # map source name
    gapminder_sg["source"] = SOURCES_NAMES["gapminder_sg"]

    df = pd.DataFrame(pd.concat([df, gapminder_sg], ignore_index=True))
    return df


def filter_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Make sure that all rows make sense.

    - Remove rows with population = 0.
    - Remove datapoints for the Netherland Antilles after 2010 (it was dissolved then), as HYDE has data after that year.
    """
    log.info("population: filter rows...")
    # remove datapoints with population = 0
    df = cast(pd.DataFrame, df[df["population"] > 0].copy())
    # remove datapoints for the Netherland Antilles after 2010 (it was dissolved then)
    df = df[~((df["country"] == "Netherlands Antilles") & (df["year"] > 2010))]
    return df


def set_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Assign adequate dtypes to columns."""
    log.info("population: setting dtypes...")
    # correct dtypes
    df["population"] = df["population"].astype("int64")
    df["year"] = df["year"].astype(int)
    df["source"] = df["source"].astype("category")
    return df


def add_world_population_share(df: pd.DataFrame) -> pd.DataFrame:
    """Obtain world's population share for each country/region and year."""
    log.info("population: adding world population share...")
    # Add a metric "% of world population"
    world_pop = df.loc[df["country"] == "World", ["year", "population"]].rename(columns={"population": "world_pop"})
    df = df.merge(world_pop, on="year", how="left")
    df["world_pop_share"] = (100 * df["population"].div(df.world_pop)).round(2)
    df = df.drop(columns="world_pop")
    return df


def df_to_table(df: pd.DataFrame) -> Table:
    """Create table from dataframe."""
    log.info("population: converting df to table...")
    # fine tune df (sort rows, columns, set index)
    df = df.set_index(["country", "year"]).sort_index()[["population", "world_pop_share", "source"]]
    # create table, sort rows
    tb = Table(df, short_name="population")
    # add metadata to columns
    tb["population"].title = "Total population (Gapminder, HYDE & UN)"
    tb["world_pop_share"].title = "Share of World Population"
    tb["source"].title = "Source"
    # underscore
    tb = underscore_table(tb)
    return tb
