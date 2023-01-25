"""
This is the code to integrate three sheets of the Maddison Database (GDP, GDP pc and population) into one dataset on meadow.
This dataset is only used to estimate growth of global GDP before 1820, so only global values are kept.

"""

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import PathFinder
from etl.snapshot import Snapshot
from etl.steps.data.converters import convert_snapshot_metadata

SNAPSHOT_VERSION = "2022-12-23"
MEADOW_VERSION = SNAPSHOT_VERSION

log = get_logger()

# naming conventions
N = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("maddison_database.start")

    # retrieve snapshot
    snap = Snapshot(f"ggdc/{SNAPSHOT_VERSION}/maddison_database.xlsx")
    df_pop = pd.read_excel(snap.path, sheet_name="Population", skiprows=2)
    df_gdp = pd.read_excel(snap.path, sheet_name="GDP", skiprows=2)
    df_gdppc = pd.read_excel(snap.path, sheet_name="PerCapita GDP", skiprows=2)

    # As this is a bespoke dataset I am only keeping the World data for now
    # Population sheet
    df_pop = df_pop.rename(columns={"Unnamed: 0": "year", "World Total": "population"})
    df_pop = df_pop[["year", "population"]]
    df_pop = df_pop.dropna().reset_index(drop=True)
    df_pop["year"] = df_pop["year"].astype(int)

    # GDP sheet
    df_gdp = df_gdp.rename(columns={"Unnamed: 0": "year", "World Total": "gdp"})
    df_gdp = df_gdp[["year", "gdp"]]
    df_gdp = df_gdp.dropna().reset_index(drop=True)
    df_gdp["year"] = df_gdp["year"].astype(int)

    # GDP per capita sheet
    df_gdppc = df_gdppc.rename(columns={"Unnamed: 0": "year", "World Total": "gdp_per_capita"})
    df_gdppc = df_gdppc[["year", "gdp_per_capita"]]
    df_gdppc = df_gdppc.dropna().reset_index(drop=True)
    df_gdppc["year"] = df_gdppc["year"].astype(int)

    # Merge all these sheets
    df = pd.merge(df_gdp, df_gdppc, on="year", how="outer", sort=True)
    df = pd.merge(df, df_pop, on="year", how="outer", sort=True)

    # Adjust country and population columns and reorder
    df["country"] = "World"
    df["population"] = df["population"].astype(int)
    df = df[["year", "country", "gdp", "gdp_per_capita", "population"]]

    # create new dataset and reuse walden metadata
    ds = Dataset.create_empty(dest_dir, metadata=convert_snapshot_metadata(snap.metadata))
    ds.metadata.version = MEADOW_VERSION

    # # create table with metadata from dataframe and underscore all columns
    tb = Table(df, short_name=snap.metadata.short_name, underscore=True)

    # add table to a dataset
    ds.add(tb)

    # finally save the dataset
    ds.save()

    log.info("maddison_database.end")
