"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("tourism_departures_per_thousand.start")

    #
    # Load inputs.
    #
    # Load garden datasets.
    log.info("tourism_departures_per_thousand.load_datasets")
    ds_wdi: Dataset = paths.load_dependency("wdi")
    ds_population: Dataset = paths.load_dependency("population")

    # Read tables with relevant columns
    log.info("tourism_departures_per_thousand.get_tables")
    tb_wdi = ds_wdi["wdi"][["st_int_dprt"]].dropna()
    tb_population = ds_population["population"][["population"]]

    # Create a dataframe with data from the table.
    log.info("tourism_departures_per_thousand.get_dfs")
    df_wdi = pd.DataFrame(tb_wdi)
    df_population = pd.DataFrame(tb_population)

    #
    # Process data.
    #
    # Fix wrong income group namings in WDI dataset
    log.info("tourism_departures_per_thousand.fix_wdi")
    mapping = {
        "High income": "High-income countries",
        "Low income": "Low-income countries",
        "Lower middle income": "Lower-middle-income countries",
        "Upper middle income": "Upper-middle-income countries",
    }
    df_wdi = df_wdi.rename(index=mapping)
    # Merge WDI data with population data
    log.info("tourism_departures_per_thousand.merge_and_ratio")
    df = df_wdi.merge(df_population, left_index=True, right_index=True)
    # Estimate ratio
    df["tourism_departures_per_thousand"] = df["st_int_dprt"] / df["population"] * 1000
    # Only keep ratio column
    df = df[["tourism_departures_per_thousand"]]
    # Create a new table with the processed data.
    tb_garden = Table(df, short_name="tourism_departures_per_thousand")
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    log.info("tourism_departures_per_thousand.create_dataset")
    ds_garden = create_dataset(dest_dir, tables=[tb_garden])
    # Fill metadata fields dynamically based on two building datasets
    log.info("tourism_departures_per_thousand.add_metadata")
    ds_garden.metadata.description = """
    Number of tourist departures per 1000 was derived based on the number of departures per year, divided by population figures from the World Bank's World Development Indicators (WDI).

    Number of tourist departures sourced from the World Bank's World Development Indicators (WDI). Available at: https://data.worldbank.org/indicator/ST.INT.DPRT.

    Population data sourced from our core population dataset. More info at https://ourworldindata.org/population-sources.
    """
    ds_garden.metadata.title = "Number of tourist departures per 1000 (various sources, 2023)"
    ds_garden.metadata.sources = ds_wdi.metadata.sources + ds_population.metadata.sources
    ds_garden.metadata.licenses = ds_wdi.metadata.licenses + ds_population.metadata.licenses
    # Some of metadata (variable description comes from YAML file)
    ds_garden.update_metadata(paths.metadata_path)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("tourism_departures_per_thousand.end")
