"""OMM population dataset.

This dataset contains population data from various sources. It also includes former countries.

Four sources are used overall:

- After 1950:
    - UN WPP (2022)
- Before 1950:
    - Gapminder (v7): This is prioritised over HYDE. Gapminder v7 relies on multiple sources:
        - CLIO/Maddison: https://clio-infra.eu/Indicators/TotalPopulation.html
        - Gapminder (v3): https://www.gapminder.org/data/documentation/gd003/
    - HYDE (v3.2)
    - Gapminder (Systema Globalis):
        Provides data on former countries, and complements other sources with data on missing years for some countries.
        More on this dataset please refer to module gapminder_sg.
"""

import json
from typing import List

import owid.catalog.processing as pr
from gapminder import load_gapminder
from gapminder_sg import (
    load_gapminder_sys_glob_complement,
    load_gapminder_sys_glob_former,
)
from hyde import load_hyde
from owid.catalog import Table
from structlog import get_logger
from unwpp import load_unwpp

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# naming conventions
paths = PathFinder(__file__)

# sources names
# this dictionary maps source short names to complete source names
SOURCES_NAMES = {
    "unwpp": "United Nations - World Population Prospects (2022) (https://population.un.org/wpp/Download/Standard/Population/)",
    "gapminder_v7": "Gapminder v7 (2022) (https://www.gapminder.org/data/documentation/gd003/)",
    "gapminder_sg": "Gapminder - Systema Globalis (2023) (https://github.com/open-numbers/ddf--gapminder--systema_globalis)",
    "hyde": "HYDE v3.2 (2017) (https://dataportaal.pbl.nl/downloads/HYDE/)",
}
# Former countries
## These countries are added by aggregating their successors' values, and using regions.
FORMER_COUNTRIES_CODES = {"OWID_USS"}


def run(dest_dir: str) -> None:
    log.info("population.start")

    # Load regions table
    ds_regions = paths.load_dataset("regions")
    tb_regions = ds_regions["regions"]

    tb = make_table(tb_regions)

    # keep original table with all origins, population table has only one origin
    # defined in YAML file
    tb_original = tb.copy().update_metadata(short_name="population_original")

    # create dataset
    log.info("population: create dataset")
    ds = create_dataset(dest_dir, tables=[tb, tb_original])

    # save dataset
    ds.save()

    log.info("population.end")


def make_table(tb_regions: Table) -> Table:
    tb = (
        load_data()
        .pipe(select_source)
        .pipe(add_regions)
        .pipe(add_world)
        .pipe(add_historical_regions, tb_regions)
        .pipe(fix_anomalies)
        .pipe(set_dtypes)
        .pipe(add_world_population_share)
        .pipe(postprocess_table)
    )
    return tb


def load_data() -> Table:
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
    tb = pr.concat([gapminder, gapminder_comp, hyde, unwpp], ignore_index=True)
    return tb


def select_source(df: Table) -> Table:
    """Select adequate source for each country-year.

    Rows are selected based on the following relevance scale: "unwpp" > "gapminder_v7" > "hyde"
    """
    log.info("population: selecting source...")
    df = df.loc[df["population"] > 0]

    # If a country has UN data, then remove all non-UN data after 1949
    has_un_data = set(df.loc[df["source"] == "unwpp", "country"])
    df = df.loc[~((df["country"].isin(has_un_data)) & (df["year"] >= 1950) & (df["source"] != "unwpp"))]

    # If a country has Gapminder data, then remove all non-Gapminder data between 1800 and 1949
    has_gapminder_data = set(df.loc[df["source"] == "gapminder_v7", "country"])
    df = df.loc[
        ~(
            (df["country"].isin(has_gapminder_data))
            & (df["year"] >= 1800)
            & (df["year"] <= 1949)
            & (df["source"] != "gapminder_v7")
        )
    ]

    # Test if all countries have only one row per year
    _assert_unique(df, subset=["country", "year"])

    # map to source full names
    df["source"] = df["source"].map(SOURCES_NAMES)
    return df


def _assert_unique(df: Table, subset: List[str]) -> None:
    """Ensure that dataframe has only one row per columns in subset"""
    # NOTE: this could be moved to helpers
    df_deduped = df.drop_duplicates(subset=subset)
    if df.shape != df_deduped.shape:
        diff = df[~df.index.isin(df_deduped.index)]
        raise AssertionError(f"Duplicate rows:\n {diff}")


def add_regions(df: Table) -> Table:
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


def add_world(df: Table) -> Table:
    """Add world aggregate.

    We do this by adding the values for all continents.

    Note that for some years, there is data available for 'World'. This is because some of the datasets
    contain 'World' data but others don't.
    """
    log.info("population: adding World...")
    df_ = df.copy()
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
    df = pr.concat([df, df_], ignore_index=True).sort_values(["country", "year"])

    # add sources for world
    df.loc[df["country"] == "World", "source"] = "; ".join(sorted(SOURCES_NAMES.values()))
    return df


def add_historical_regions(df: Table, tb_regions: Table) -> Table:
    """Add historical regions.

    Historical regions are added using different techniques:

    1. Systema Globalis from Gapminder contains historical regions. We add them to the data. These include
    Yugoslavia, USSR, etc. Note that this is added after regions and world regions have been obtained, to avoid double counting.
    2. Add historical regions by grouping and summing current countries.
    """

    # 1. Add from Systema Globalis
    log.info("population: loading data (Gapminder Systema Globalis)")
    gapminder_sg = load_gapminder_sys_glob_former()
    # map source name
    gapminder_sg["source"] = SOURCES_NAMES["gapminder_sg"]
    # Add to main table
    tb = pr.concat([df, gapminder_sg], ignore_index=True)

    # 2. Add historical regions by grouping and summing current countries.
    for ccode in FORMER_COUNTRIES_CODES:
        # Get former country name and end year (dissolution)
        former_country_name = tb_regions.loc[ccode, "name"]
        end_year = tb_regions.loc[ccode, "end_year"]
        # Sanity check
        assert former_country_name not in set(
            tb["country"]
        ), f"{former_country_name} already in table (either import it via Systema Globalis or manual aggregation)!"
        # Get list of country successors (equivalent of former state nowadays) and end year (dissolution of former state)
        ccodes_successors = json.loads(tb_regions.loc[ccode, "successors"])
        successor_names = tb_regions.loc[ccodes_successors, "name"].tolist()
        # Filter table accordingly
        tb_ = tb[(tb["year"] <= end_year) & (tb["country"].isin(successor_names))]
        # Filter rows (only preserve years where all countries have data)
        year_filter = tb_.groupby("year")["country"].nunique() == len(successor_names)
        year_filter = year_filter[year_filter].index.tolist()
        tb_ = tb_[tb_["year"].isin(year_filter)]
        # Perform operations
        tb_ = tb_.groupby("year", as_index=False, observed=True).agg(
            {"population": sum, "source": lambda x: "; ".join(sorted(set(x)))}
        )
        tb_["country"] = former_country_name
        # Add to main table
        tb = pr.concat([tb, tb_], ignore_index=True)
    return tb


def fix_anomalies(df: Table) -> Table:
    """Make sure that all rows make sense.

    - Remove rows with population = 0.
    - Remove datapoints for the Netherland Antilles after 2010 (it was dissolved then), as HYDE has data after that year.
    """
    log.info("population: filter rows...")
    # remove datapoints with population = 0
    df = df[df["population"] > 0].copy()
    # remove datapoints for the Netherland Antilles after 2010 (it was dissolved then)
    df = df[~((df["country"] == "Netherlands Antilles") & (df["year"] > 2010))]
    return df


def set_dtypes(df: Table) -> Table:
    """Assign adequate dtypes to columns."""
    log.info("population: setting dtypes...")
    # correct dtypes
    df["population"] = df["population"].astype("int64")
    df["year"] = df["year"].astype(int)
    df["source"] = df["source"].astype("category")
    return df


def add_world_population_share(df: Table) -> Table:
    """Obtain world's population share for each country/region and year."""
    log.info("population: adding world population share...")
    # Add a metric "% of world population"
    world_pop = df.loc[df["country"] == "World", ["year", "population"]].rename(columns={"population": "world_pop"})
    df = df.merge(world_pop, on="year", how="left")
    df["world_pop_share"] = 100 * df["population"].div(df.world_pop)
    df = df.drop(columns="world_pop")
    return df


def postprocess_table(df: Table) -> Table:
    # fine tune df (sort rows, columns, set index)
    df = df.set_index(["country", "year"], verify_integrity=True).sort_index()[
        ["population", "world_pop_share", "source"]
    ]
    return df
