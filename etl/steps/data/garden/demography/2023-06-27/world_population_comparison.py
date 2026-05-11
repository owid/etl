from owid.catalog import Table
from owid.catalog import processing as pr
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

paths = PathFinder(__file__)
log = get_logger()


def run(dest_dir: str) -> None:
    log.info("world_population_comparison: start")
    snap = Snapshot("fasttrack/2023-06-19/world_population_comparison.csv")
    tb = snap.read_csv()

    tb_hyde = get_hyde_32()
    tb_gapminder = get_gapminder_v7()
    tb_un = get_un_2022()
    tb_owid = get_owid()
    tb = pr.concat([tb, tb_hyde, tb_gapminder, tb_un, tb_owid], ignore_index=True)
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    ds = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)
    ds.save()
    log.info("world_population_comparison: end")


def get_hyde_32() -> Table:
    log.info("world_population_comparison: load hyde")
    ds = paths.load_dataset("baseline", namespace="hyde")
    tb = ds["population"].reset_index()
    tb = tb.groupby("year", as_index=False)[["population"]].sum()
    tb["country"] = "HYDE 3.2 (2017)"
    return tb.rename(columns={"population": "world_population"})


def get_gapminder_v7() -> Table:
    log.info("world_population_comparison: load gapminder")
    ds = paths.load_dataset("population", namespace="gapminder")
    tb = ds["population"].reset_index()
    tb = tb.groupby("year", as_index=False)[["population"]].sum()
    tb["country"] = "Gapminder v7 (2022)"
    return tb.rename(columns={"population": "world_population"})


def get_un_2022() -> Table:
    """Load UN 2022 data — historical estimates and medium variant projections."""
    log.info("world_population_comparison: load un wpp")
    ds = paths.load_dataset("un_wpp", namespace="un")
    tb = ds["population"].reset_index()

    common = (tb["location"] == "World") & (tb["sex"] == "all") & (tb["age"] == "all") & (tb["metric"] == "population")
    tb_estimates = tb[common & (tb["variant"] == "estimates")][["year", "value"]]
    tb_estimates["country"] = "UN (2022 revision)"
    tb_proj = tb[common & (tb["variant"] == "medium")][["year", "value"]]
    tb_proj["country"] = "UN, medium variant projection (2022 revision)"

    tb = pr.concat([tb_estimates, tb_proj], ignore_index=True)
    return tb.rename(columns={"value": "world_population"})


def get_owid() -> Table:
    """Load OWID population dataset."""
    log.info("world_population_comparison: load owid")
    ds = paths.load_dataset("population", namespace="demography")
    tb = ds["population"].reset_index()
    tb = tb[tb["country"] == "World"]
    tb["country"] = "OWID based on HYDE 3.2, Gapminder v7 and UN 2022"
    tb = tb[["country", "year", "population"]]
    return tb.rename(columns={"population": "world_population"})
