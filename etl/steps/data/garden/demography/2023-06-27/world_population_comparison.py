from typing import cast

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

P = PathFinder(__file__)
log = get_logger()


def run(dest_dir: str) -> None:
    log.info("world_population_comparison: start")
    # load snapshot
    data = pd.read_csv(Snapshot("fasttrack/2023-06-19/world_population_comparison.csv").path)

    # create empty dataframe and table
    tb = Table(data, short_name=P.short_name)

    # Add sources
    tb_hyde = get_hyde_32()
    tb_gapminder = get_gapminder_v7()
    tb_un = get_un_2022()
    tb_owid = get_owid()
    tb = (
        pd.concat([tb, tb_hyde, tb_gapminder, tb_un, tb_owid], ignore_index=True)
        .set_index(["country", "year"])
        .sort_index()
    )
    tb.metadata.short_name = P.short_name

    # add table, update metadata from *.meta.yml and save
    ds = create_dataset(dest_dir, tables=[tb])
    ds.save()
    log.info("world_population_comparison: end")


def get_hyde_32() -> Table:
    """Load HYDE 3.2 data and format accordingly."""
    log.info("world_population_comparison: load hyde")
    ds = cast(Dataset, P.load_dependency(namespace="hyde", short_name="baseline"))
    tb = ds["population"].reset_index()
    # Get only World data, add source name
    tb = tb.groupby("year", as_index=False)[["population"]].sum()
    tb.loc[:, "country"] = "HYDE 3.2 (2017)"

    # Rename population column
    tb = tb.rename(columns={"population": "world_population"})
    return tb


def get_gapminder_v7() -> Table:
    """Load Gapminder v7 data and format accordingly."""
    log.info("world_population_comparison: load gapminder")
    ds = cast(Dataset, P.load_dependency(namespace="gapminder", short_name="population"))
    tb = ds["population"].reset_index()
    # Get only World data, add source name
    tb = tb.groupby("year", as_index=False)[["population"]].sum()
    tb.loc[:, "country"] = "Gapminder v7 (2022)"

    # Rename population column
    tb = tb.rename(columns={"population": "world_population"})
    return tb


def get_un_2022() -> Table:
    """Load UN 2022 data and format accordingly.

    Both historical estimates and medium variant projections are loaded.
    """
    log.info("world_population_comparison: load un wpp")
    ds = cast(Dataset, P.load_dependency(namespace="un", short_name="un_wpp"))
    tb = ds["population"].reset_index()
    # Get estimates: only World data, all sexes and ages, add source name
    tb_estimates = tb[
        (tb["location"] == "World")
        & (tb["sex"] == "all")
        & (tb["age"] == "all")
        & (tb["variant"] == "estimates")
        & (tb["metric"] == "population")
    ]
    tb_estimates = tb_estimates[["year", "value"]]
    tb_estimates.loc[:, "country"] = "UN (2022 revision)"
    # Get projections: only World data, all sexes and ages, add source name
    tb_proj = tb[
        (tb["location"] == "World")
        & (tb["sex"] == "all")
        & (tb["age"] == "all")
        & (tb["variant"] == "medium")
        & (tb["metric"] == "population")
    ]
    tb_proj = tb_proj[["year", "value"]]
    tb_proj.loc[:, "country"] = "UN, medium variant projection (2022 revision)"

    # Merge estimates & projections
    tb = pd.concat([tb_estimates, tb_proj], ignore_index=True)

    # Rename population column
    tb = tb.rename(columns={"value": "world_population"})  # type: ignore
    return tb


def get_owid() -> Table:
    """Load Gapminder v7 data and format accordingly."""
    log.info("world_population_comparison: load owid")
    ds = cast(Dataset, P.load_dependency(namespace="demography", short_name="population"))
    tb = ds["population"].reset_index()
    # Get only World data, add source name
    tb = tb[tb["country"] == "World"]
    tb["country"] = "OWID based on HYDE 3.2, Gapminder v7 and UN 2022"
    tb = tb[["country", "year", "population"]]

    # Rename population column
    tb = tb.rename(columns={"population": "world_population"})

    return tb
